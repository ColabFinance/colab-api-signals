from __future__ import annotations

import logging
from typing import Any, List, Optional

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from core.domain.entities.trade_signal_entity import TradeSignalEntity
from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
from core.domain.entities.trade_strategy_runtime_snapshot_entity import (
    TradeStrategyRuntimeSnapshotEntity,
)
from core.domain.enums.trade_enums import TradeSignalStatus, TradeSignalType
from core.repositories.trade_signal_repository import TradeSignalRepository
from core.repositories.trade_strategy_repository import TradeStrategyRepository
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)
from core.services.atr_two_stage_trade_strategy_service import AtrTwoStageTradeStrategyService
from core.services.trade_idempotency_key_service import TradeIdempotencyKeyService


class EvaluateActiveTradeStrategiesUseCase:
    """
    Evaluate all active trade strategies for a closed 1m candle.

    This use case can work with either:
    - a preloaded candle window passed by the caller
    - a fallback fetch from api-market-data

    For each evaluated strategy, it persists:
    - the current runtime snapshot
    - the optional emitted trade signal

    The latest saved runtime snapshot is also used as the initial operational
    state for the next evaluation so that the strategy remains stateful.
    """

    def __init__(
        self,
        *,
        strategy_repo: TradeStrategyRepository,
        signal_repo: TradeSignalRepository,
        runtime_snapshot_repo: TradeStrategyRuntimeSnapshotRepository,
        market_data_client: Optional[MarketDataHttpClient] = None,
        strategy_service: Optional[AtrTwoStageTradeStrategyService] = None,
        idempotency_service: Optional[TradeIdempotencyKeyService] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the use case.
        """
        self._strategy_repo = strategy_repo
        self._signal_repo = signal_repo
        self._runtime_snapshot_repo = runtime_snapshot_repo
        self._market_data = market_data_client or MarketDataHttpClient.from_settings()
        self._strategy_service = strategy_service or AtrTwoStageTradeStrategyService()
        self._idempotency = idempotency_service or TradeIdempotencyKeyService()
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    async def execute_for_stream(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
        candles: Optional[List[dict]] = None,
        strategies: Optional[List[TradeStrategyEntity]] = None,
    ) -> List[TradeSignalEntity]:
        """
        Evaluate all active trade strategies for a stream_key and candle close timestamp.

        Returns:
            List of generated or reused trade signals.
        """
        normalized_stream_key = str(stream_key).strip().lower()

        resolved_strategies = strategies or await self._strategy_repo.get_active_by_stream_key(normalized_stream_key)
        if not resolved_strategies:
            return []

        resolved_candles = candles
        if resolved_candles is None:
            required_limit = self._resolve_required_limit(resolved_strategies)
            resolved_candles = await self._market_data.list_candles(
                stream_key=normalized_stream_key,
                limit=int(required_limit),
            )

        if not resolved_candles:
            self._logger.warning("No candles returned for stream_key=%s ts=%s", normalized_stream_key, ts)
            return []

        out: List[TradeSignalEntity] = []

        for strategy in resolved_strategies:
            try:
                previous_snapshot = await self._runtime_snapshot_repo.get_latest_by_strategy_id(str(strategy.id))

                result = self._strategy_service.evaluate_latest(
                    strategy=strategy,
                    candles=resolved_candles,
                    previous_snapshot=previous_snapshot,
                )
                if result is None:
                    continue

                runtime_snapshot_dict = result.get("runtime_snapshot")
                if runtime_snapshot_dict is not None:
                    runtime_snapshot = TradeStrategyRuntimeSnapshotEntity(**runtime_snapshot_dict)
                    await self._runtime_snapshot_repo.upsert(runtime_snapshot)

                signal_dict = result.get("signal")
                if signal_dict is None:
                    continue

                if int(signal_dict["ts"]) != int(ts):
                    continue

                signal_type = self._normalize_signal_type(signal_dict.get("signal_type"))
                idempotency_key = self._idempotency.build(
                    strategy_id=str(strategy.id),
                    stream_key=str(strategy.stream_key),
                    ts=int(signal_dict["ts"]),
                    signal_type=signal_type.value,
                )

                signal = TradeSignalEntity(
                    strategy_id=str(strategy.id),
                    stream_key=str(strategy.stream_key),
                    symbol=str(strategy.symbol).upper(),
                    interval=str(strategy.interval).lower(),
                    ts=int(signal_dict["ts"]),
                    signal_type=signal_type,
                    status=TradeSignalStatus.PENDING,
                    idempotency_key=idempotency_key,
                    payload={
                        "source": str(source).strip().lower(),
                        "event": self._enum_value(signal_dict.get("event")),
                        "close": signal_dict.get("close"),
                        "position_side": self._enum_value(signal_dict.get("position_side")),
                        "desired_side": self._enum_value(signal_dict.get("desired_side")),
                        "atr": signal_dict.get("atr"),
                        "atr_pct": signal_dict.get("atr_pct"),
                        "atr_threshold_source": self._enum_value(signal_dict.get("atr_threshold_source")),
                        "atr_low_threshold_active": signal_dict.get("atr_low_threshold_active"),
                        "atr_high_threshold_active": signal_dict.get("atr_high_threshold_active"),
                        "regime_trend_ma": signal_dict.get("regime_trend_ma"),
                        "regime_structure_ma": signal_dict.get("regime_structure_ma"),
                        "regime_allows_desired": signal_dict.get("regime_allows_desired"),
                        "setup_reference_price": signal_dict.get("setup_reference_price"),
                        "setup_reference_atr": signal_dict.get("setup_reference_atr"),
                        "setup_reference_atr_value_for_threshold": signal_dict.get("setup_reference_atr_value_for_threshold"),
                        "setup_age_bars": signal_dict.get("setup_age_bars"),
                        "entry_reference_price": signal_dict.get("entry_reference_price"),
                        "entry_atr": signal_dict.get("entry_atr"),
                        "entry_atr_value_for_threshold": signal_dict.get("entry_atr_value_for_threshold"),
                        "bars_in_trade": signal_dict.get("bars_in_trade"),
                        "trailing_active": signal_dict.get("trailing_active"),
                        "open_trade_loss_pct": signal_dict.get("open_trade_loss_pct"),
                        "strategy_type": self._enum_value(strategy.strategy_type),
                        "execution_target": self._enum_value(strategy.execution_target),
                        "execution_account_id": strategy.execution_account_id,
                        "runtime_state": self._enum_value(signal_dict.get("runtime_state")),
                    },
                )

                stored = await self._signal_repo.upsert_by_idempotency_key(signal)
                out.append(stored)
            except Exception as exc:
                self._logger.exception(
                    "Failed to evaluate trade strategy. strategy_id=%s stream_key=%s ts=%s err=%s",
                    getattr(strategy, "id", None),
                    normalized_stream_key,
                    ts,
                    exc,
                )

        return out

    def _resolve_required_limit(self, strategies: List[TradeStrategyEntity]) -> int:
        """
        Resolve the largest candle window required among all strategies in the same stream.
        """
        if not strategies:
            return 100

        max_need = 0
        for strategy in strategies:
            max_need = max(max_need, int(self._strategy_service.required_bars(strategy)))

        return max(50, int(max_need))

    def _normalize_signal_type(self, value: Any) -> TradeSignalType:
        """
        Normalize an arbitrary value to a TradeSignalType.
        """
        if isinstance(value, TradeSignalType):
            return value
        return TradeSignalType(str(value).strip().upper())

    def _enum_value(self, value: Any) -> Any:
        """
        Return the raw enum value when the object is an enum-like instance.
        """
        return value.value if hasattr(value, "value") else value