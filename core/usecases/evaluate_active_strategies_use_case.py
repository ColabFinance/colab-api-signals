from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Callable, List, Optional, Sequence

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from core.domain.entities.lp_indicator_snapshot_entity import LpIndicatorSnapshotEntity
from core.domain.entities.signal_entity import SignalEntity, SignalStep
from core.domain.entities.strategy_entity import StrategyEntity
from core.domain.enums.signal_enums import SignalStatus, SignalType
from core.services.lp_indicator_calculation_service import LpIndicatorCalculationService
from core.services.simple_wide_lp_strategy_service import SimpleWideLPStrategyService

from ..services.strategy_reconciler_service import StrategyReconcilerService
from ..repositories.strategy_repository import StrategyRepository
from ..repositories.strategy_episode_repository import StrategyEpisodeRepository
from ..repositories.signal_repository import SignalRepository


class EvaluateActiveStrategiesUseCase:
    """
    Orchestrates LP evaluation only.

    Strategy decision logic lives in SimpleWideLPStrategyService.
    This use case only:
      - loads active strategies
      - loads current episode
      - computes LP indicators locally from candle history
      - delegates the decision to the strategy service
      - persists close/open transitions
      - persists minute-by-minute runtime history
      - emits a PENDING signal when a vault action is required
    """

    def __init__(
        self,
        strategy_repo: StrategyRepository,
        episode_repo: StrategyEpisodeRepository,
        signal_repo: SignalRepository,
        reconciling_service: StrategyReconcilerService,
        lp_client: PipelineHttpClient,
        logger: Optional[logging.Logger] = None,
        on_signal_created: Optional[Callable[[], None]] = None,
        indicator_service: Optional[LpIndicatorCalculationService] = None,
    ):
        self._strategy_repo = strategy_repo
        self._episode_repo = episode_repo
        self._signal_repo = signal_repo
        self._reconciler = reconciling_service
        self._lp_client = lp_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._on_signal_created = on_signal_created
        self.market_data = MarketDataHttpClient.from_settings()
        self._lp_strategy = SimpleWideLPStrategyService(logger=self._logger)
        self._indicator_service = indicator_service or LpIndicatorCalculationService()

    def _strategy_ref_id(self, strategy: StrategyEntity) -> str:
        if getattr(strategy, "id", None):
            return str(strategy.id).strip()
        return str(strategy.name).strip()

    def _strategy_symbol(self, strategy: StrategyEntity, indicator_snapshot: LpIndicatorSnapshotEntity) -> str:
        raw = str(getattr(strategy, "symbol", "") or "").strip().upper()
        if raw:
            return raw
        return str(indicator_snapshot.symbol).strip().upper()

    async def _resolve_exec_price(self, indicator_snapshot: LpIndicatorSnapshotEntity) -> float:
        """
        Keeps current behavior as first choice, but falls back to snapshot close
        so the strategy still works if that external price call fails.
        """
        try:
            price = await self.market_data.get_token_price_usd(
                chain="base",
                token_address="0x4200000000000000000000000000000000000006",
            )
            return float(price["price_usd"])
        except Exception:
            return float(indicator_snapshot.close)

    def required_bars_for_strategy(self, strategy: StrategyEntity) -> int:
        params = strategy.params

        return int(
            self._indicator_service.required_bars_for(
                atr_window=int(params.atr_period),
                entry_trend_ma_window=int(params.entry_trend_ma_window or 0),
            )
        )

    def _build_indicator_snapshot(
        self,
        *,
        strategy: StrategyEntity,
        candles: Sequence[dict],
    ) -> Optional[LpIndicatorSnapshotEntity]:
        params = strategy.params

        return self._indicator_service.compute_snapshot_for_last(
            candles=candles,
            atr_window=int(params.atr_period),
            entry_trend_ma_window=int(params.entry_trend_ma_window or 0),
        )

    async def _emit_signal(
        self,
        *,
        strategy: StrategyEntity,
        symbol: str,
        ts: int,
        episode,
        last_episode,
    ) -> None:
        strategy_ref_id = self._strategy_ref_id(strategy)

        signal_plan = await self._reconciler.reconcile(strategy_ref_id, episode, symbol)
        if not signal_plan:
            return

        legacy_cfg_key = (
            getattr(strategy, "indicator_set_id", None)
            or getattr(strategy, "cfg_hash", None)
            or getattr(strategy, "stream_key", None)
            or strategy.name
        )

        signal = SignalEntity(
            strategy_id=strategy_ref_id,
            indicator_set_id=str(legacy_cfg_key),
            cfg_hash=str(legacy_cfg_key),
            symbol=symbol,
            ts=ts,
            signal_type=SignalType(signal_plan["signal_type"]),
            status=SignalStatus.PENDING,
            attempts=0,
            steps=[SignalStep(**step) for step in signal_plan["steps"]],
            episode=episode,
            last_episode=last_episode,
        )
        await self._signal_repo.upsert_signal(signal)
        if self._on_signal_created:
            self._on_signal_created()

    async def execute_for_stream(
        self,
        *,
        stream_key: str,
        candles: Sequence[dict],
        strategies: Optional[List[StrategyEntity]] = None,
    ) -> None:
        normalized_stream_key = str(stream_key).strip().lower()

        if strategies is None:
            strategies = await self._strategy_repo.get_active_by_stream_key(normalized_stream_key)

        if not strategies:
            return

        for strat in strategies:
            if strat.alias is None:
                continue

            indicator_snapshot = self._build_indicator_snapshot(strategy=strat, candles=candles)
            if indicator_snapshot is None:
                continue

            strategy_ref_id = self._strategy_ref_id(strat)
            symbol = self._strategy_symbol(strat, indicator_snapshot)

            P_signal = float(indicator_snapshot.close)
            ts = int(indicator_snapshot.ts)
            P_exec = await self._resolve_exec_price(indicator_snapshot)

            current = await self._episode_repo.get_open_by_strategy(strategy_ref_id)

            if current is None and strategy_ref_id != str(strat.name):
                current = await self._episode_repo.get_open_by_strategy(str(strat.name))

            if current is None:
                new_ep = self._lp_strategy.build_initial_episode(
                    strategy=strat,
                    indicator_snapshot=indicator_snapshot,
                    symbol=symbol,
                    P_signal=P_signal,
                    P_exec=P_exec,
                    ts=ts,
                )
                new_ep = await self._episode_repo.open_new(new_ep)

                runtime = self._lp_strategy.build_initial_runtime(
                    strategy=strat,
                    episode=new_ep,
                    indicator_snapshot=indicator_snapshot,
                    P_exec=P_exec,
                )
                await self._episode_repo.upsert_runtime(runtime)

                await self._emit_signal(
                    strategy=strat,
                    symbol=symbol,
                    ts=ts,
                    episode=new_ep,
                    last_episode=None,
                )
                continue

            result = self._lp_strategy.evaluate_current_episode(
                strategy=strat,
                current=current,
                indicator_snapshot=indicator_snapshot,
                symbol=symbol,
                P_signal=P_signal,
                P_exec=P_exec,
                ts=ts,
            )

            if result.current_updates:
                await self._episode_repo.update_partial(current.id, result.current_updates)

            if result.runtime is not None:
                await self._episode_repo.upsert_runtime(result.runtime)

            if not result.should_close or result.new_episode is None:
                continue

            now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

            close_fields = {
                "close_time": ts,
                "close_time_iso": now_iso,
                "close_reason": result.close_reason,
                "close_price_exec": P_exec,
                "close_price_signal": P_signal,
            }

            await self._episode_repo.close_episode(current.id, close_fields)

            current.close_time = ts
            current.close_time_iso = now_iso
            current.close_reason = result.close_reason
            current.close_price_exec = P_exec
            current.close_price_signal = P_signal

            new_ep = await self._episode_repo.open_new(result.new_episode)

            await self._emit_signal(
                strategy=strat,
                symbol=symbol,
                ts=ts,
                episode=new_ep,
                last_episode=current,
            )