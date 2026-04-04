from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.redis.trade_candle_buffer_repository_redis import TradeCandleBufferRepositoryRedis
from core.domain.entities.trade_signal_entity import TradeSignalEntity
from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
from core.repositories.trade_signal_repository import TradeSignalRepository
from core.repositories.trade_strategy_repository import TradeStrategyRepository
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)
from core.repositories.trade_trigger_event_repository import TradeTriggerEventRepository
from core.services.atr_two_stage_trade_strategy_service import AtrTwoStageTradeStrategyService
from core.usecases.evaluate_active_trade_strategies_use_case import (
    EvaluateActiveTradeStrategiesUseCase,
)


class ProcessTradeCandleClosedEventUseCase:
    """
    Process a closed trade candle event from Redis Streams or a legacy fallback trigger.
    """

    def __init__(
        self,
        *,
        trigger_event_repo: TradeTriggerEventRepository,
        strategy_repo: TradeStrategyRepository,
        signal_repo: TradeSignalRepository,
        runtime_snapshot_repo: TradeStrategyRuntimeSnapshotRepository,
        candle_buffer_repo: TradeCandleBufferRepositoryRedis,
        market_data_client: MarketDataHttpClient,
        strategy_service: Optional[AtrTwoStageTradeStrategyService] = None,
        logger: Optional[logging.Logger] = None,
        signal_waker: Optional[Callable[[], None]] = None,
        buffer_maxlen: int = 2000,
    ) -> None:
        """
        Initialize the trade candle event processor.
        """
        self._trigger_event_repo = trigger_event_repo
        self._strategy_repo = strategy_repo
        self._signal_repo = signal_repo
        self._runtime_snapshot_repo = runtime_snapshot_repo
        self._candle_buffer_repo = candle_buffer_repo
        self._market_data = market_data_client
        self._strategy_service = strategy_service or AtrTwoStageTradeStrategyService()
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._signal_waker = signal_waker
        self._buffer_maxlen = int(buffer_maxlen)

        self._evaluator = EvaluateActiveTradeStrategiesUseCase(
            strategy_repo=self._strategy_repo,
            signal_repo=self._signal_repo,
            runtime_snapshot_repo=self._runtime_snapshot_repo,
            market_data_client=self._market_data,
            strategy_service=self._strategy_service,
            logger=self._logger,
        )

    async def execute(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
        candle: Optional[Dict[str, Any]] = None,
    ) -> List[TradeSignalEntity]:
        """
        Process a single closed candle event end-to-end.
        """
        normalized_stream_key = str(stream_key).strip().lower()
        normalized_ts = int(ts)
        normalized_source = str(source).strip().lower()
        normalized_symbol = str(symbol).strip().upper()
        normalized_interval = str(interval).strip().lower()

        normalized_candle = self._normalize_candle(
            candle=candle,
            stream_key=normalized_stream_key,
            source=normalized_source,
            symbol=normalized_symbol,
            interval=normalized_interval,
            ts=normalized_ts,
        )

        if normalized_candle is not None:
            await self._candle_buffer_repo.upsert_candle(
                stream_key=normalized_stream_key,
                candle=normalized_candle,
                maxlen=self._buffer_maxlen,
            )
        
        is_new = await self._trigger_event_repo.mark_if_new(normalized_stream_key, normalized_ts)
        if not is_new:
            self._logger.debug(
                "Duplicate trade candle event ignored for evaluation. stream_key=%s ts=%s",
                normalized_stream_key,
                normalized_ts,
            )
            return []

        strategies = await self._strategy_repo.get_active_by_stream_key(normalized_stream_key)
        if not strategies:
            return []

        required_limit = self._resolve_required_limit(strategies)
        buffer_target_len = max(self._buffer_maxlen, required_limit)

        candles = await self._candle_buffer_repo.list_last(
            stream_key=normalized_stream_key,
            limit=required_limit,
        )

        if len(candles) < required_limit:
            candles = await self._bootstrap_candles(
                stream_key=normalized_stream_key,
                source=normalized_source,
                symbol=normalized_symbol,
                interval=normalized_interval,
                required_limit=required_limit,
                buffer_target_len=buffer_target_len,
                latest_candle=normalized_candle,
                ts=normalized_ts,
            )

        elif normalized_candle is not None:
            candles = self._merge_latest_candle(
                candles=candles,
                latest_candle=normalized_candle,
                limit=required_limit,
            )

        if not candles:
            self._logger.warning(
                "No candles available after buffer/bootstrap. stream_key=%s ts=%s",
                normalized_stream_key,
                normalized_ts,
            )
            return []

        signals = await self._evaluator.execute_for_stream(
            stream_key=normalized_stream_key,
            ts=normalized_ts,
            source=normalized_source,
            symbol=normalized_symbol,
            interval=normalized_interval,
            candles=candles,
            strategies=strategies,
        )

        if signals and self._signal_waker is not None:
            self._signal_waker()

        return signals

    async def _bootstrap_candles(
        self,
        *,
        stream_key: str,
        source: str,
        symbol: str,
        interval: str,
        required_limit: int,
        buffer_target_len: int,
        latest_candle: Optional[Dict[str, Any]],
        ts: int,
    ) -> List[Dict[str, Any]]:
        """
        Bootstrap the rolling buffer from api-market-data only when the hot buffer is insufficient.
        """
        bootstrap_limit = max(50, int(required_limit))
        historical = await self._market_data.list_candles(
            stream_key=stream_key,
            limit=bootstrap_limit,
        )

        normalized_historical = [
            normalized
            for normalized in (
                self._normalize_candle(
                    candle=item,
                    stream_key=stream_key,
                    source=source,
                    symbol=symbol,
                    interval=interval,
                    ts=ts,
                )
                for item in historical
            )
            if normalized is not None
        ]

        merged = normalized_historical
        if latest_candle is not None:
            merged = self._merge_latest_candle(
                candles=normalized_historical,
                latest_candle=latest_candle,
                limit=buffer_target_len,
            )

        if merged:
            await self._candle_buffer_repo.replace(
                stream_key=stream_key,
                candles=merged,
                maxlen=buffer_target_len,
            )

        return merged[-required_limit:]

    def _resolve_required_limit(self, strategies: List[TradeStrategyEntity]) -> int:
        """
        Resolve the largest candle window required among all active strategies for the stream.
        """
        max_need = 0
        for strategy in strategies:
            max_need = max(max_need, int(self._strategy_service.required_bars(strategy)))
        return max(50, int(max_need))

    def _normalize_candle(
        self,
        *,
        candle: Optional[Dict[str, Any]],
        stream_key: str,
        source: str,
        symbol: str,
        interval: str,
        ts: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Normalize an incoming candle payload into a stable dictionary shape.
        """
        if not isinstance(candle, dict):
            return None

        open_price = self._safe_float(candle.get("open"))
        high_price = self._safe_float(candle.get("high"))
        low_price = self._safe_float(candle.get("low"))
        close_price = self._safe_float(candle.get("close"))

        if open_price is None or high_price is None or low_price is None or close_price is None:
            return None

        close_time = self._safe_int(candle.get("close_time"))
        if close_time is None:
            close_time = self._safe_int(candle.get("ts"))
        if close_time is None:
            close_time = int(ts)

        open_time = self._safe_int(candle.get("open_time"))
        if open_time is None:
            open_time = max(0, int(close_time) - 60_000)

        volume = self._safe_float(candle.get("volume"))
        trades = self._safe_int(candle.get("trades"))

        return {
            "stream_key": str(candle.get("stream_key") or stream_key).strip().lower(),
            "source": str(candle.get("source") or source).strip().lower(),
            "symbol": str(candle.get("symbol") or symbol).strip().upper(),
            "interval": str(candle.get("interval") or interval).strip().lower(),
            "open_time": int(open_time),
            "close_time": int(close_time),
            "open": float(open_price),
            "high": float(high_price),
            "low": float(low_price),
            "close": float(close_price),
            "volume": float(volume or 0.0),
            "trades": int(trades or 0),
            "is_closed": self._coerce_bool(candle.get("is_closed"), default=True),
        }

    def _merge_latest_candle(
        self,
        *,
        candles: List[Dict[str, Any]],
        latest_candle: Dict[str, Any],
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Merge the latest candle into an ascending candle window and keep only the last N items.
        """
        merged_by_open_time: Dict[int, Dict[str, Any]] = {}

        for item in candles:
            open_time = self._safe_int(item.get("open_time"))
            if open_time is None:
                continue
            merged_by_open_time[int(open_time)] = item

        latest_open_time = self._safe_int(latest_candle.get("open_time"))
        if latest_open_time is not None:
            merged_by_open_time[int(latest_open_time)] = latest_candle

        merged = sorted(merged_by_open_time.values(), key=lambda x: int(x["open_time"]))
        return merged[-int(limit):]

    def _coerce_bool(self, value: Any, *, default: bool) -> bool:
        """
        Coerce an arbitrary value into a boolean.
        """
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    def _safe_int(self, value: Any) -> Optional[int]:
        """
        Safely convert an arbitrary value to int.
        """
        if value is None or value == "":
            return None
        return int(value)

    def _safe_float(self, value: Any) -> Optional[float]:
        """
        Safely convert an arbitrary value to float.
        """
        if value is None or value == "":
            return None
        return float(value)