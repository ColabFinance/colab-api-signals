from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from core.repositories.signal_repository import SignalRepository
from core.repositories.strategy_episode_repository import StrategyEpisodeRepository
from core.repositories.strategy_repository import StrategyRepository
from core.repositories.trigger_event_repository import TriggerEventRepository
from core.usecases.evaluate_active_strategies_use_case import EvaluateActiveStrategiesUseCase
from core.usecases.lp_candle_buffer_use_case import LpCandleBufferUseCase


class ProcessLpCandleClosedEventUseCase:
    """
    Process one LP candle-closed event coming from Redis Streams.
    """

    def __init__(
        self,
        *,
        trigger_repo: TriggerEventRepository,
        strategy_repo: StrategyRepository,
        episode_repo: StrategyEpisodeRepository,
        signal_repo: SignalRepository,
        candle_buffer_use_case: LpCandleBufferUseCase,
        evaluator_use_case: EvaluateActiveStrategiesUseCase,
        market_data_client: MarketDataHttpClient,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._trigger_repo = trigger_repo
        self._strategy_repo = strategy_repo
        self._episode_repo = episode_repo
        self._signal_repo = signal_repo
        self._buffer_uc = candle_buffer_use_case
        self._evaluator_uc = evaluator_use_case
        self._market_data = market_data_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    async def execute(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
        candle: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_stream_key = str(stream_key).strip().lower()

        is_new = await self._trigger_repo.mark_if_new(normalized_stream_key, int(ts))
        if not is_new:
            return

        strategies = await self._strategy_repo.get_active_by_stream_key(normalized_stream_key)
        if not strategies:
            return

        if candle is None:
            latest = await self._market_data.list_candles(stream_key=normalized_stream_key, limit=1)
            candle = latest[-1] if latest else None

        await self._buffer_uc.append_if_present(
            stream_key=normalized_stream_key,
            candle=candle,
        )

        max_need = 0
        for strategy in strategies:
            max_need = max(max_need, int(self._evaluator_uc.required_bars_for_strategy(strategy)))

        if max_need <= 0:
            self._logger.warning(
                "LP strategies on stream_key=%s do not expose local indicator params.",
                normalized_stream_key,
            )
            return

        candles = await self._buffer_uc.ensure_history(
            stream_key=normalized_stream_key,
            limit=max_need,
        )
        if not candles:
            return

        await self._evaluator_uc.execute_for_stream(
            stream_key=normalized_stream_key,
            candles=candles,
            strategies=strategies,
        )