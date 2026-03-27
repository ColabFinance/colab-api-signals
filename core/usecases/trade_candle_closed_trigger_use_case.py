from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.external.database.trade_signal_repository_mongodb import TradeSignalRepositoryMongoDB
from adapters.external.database.trade_strategy_repository_mongodb import TradeStrategyRepositoryMongoDB
from adapters.external.database.trade_strategy_runtime_snapshot_repository_mongodb import (
    TradeStrategyRuntimeSnapshotRepositoryMongoDB,
)
from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from core.usecases.evaluate_active_trade_strategies_use_case import EvaluateActiveTradeStrategiesUseCase


class TradeCandleClosedTriggerUseCase:
    """
    Orchestrate asynchronous processing of a trade candle-closed event.

    This use case protects the evaluation flow with bounded concurrency
    and isolates the trade trigger pipeline from the LP trigger pipeline.
    """

    def __init__(
        self,
        *,
        db: AsyncIOMotorDatabase,
        market_data_base_url: str,
        max_concurrency: int = 10,
        logger: Optional[logging.Logger] = None,
        signal_waker: Optional[Callable[[], None]] = None,
    ):
        """
        Initialize the trigger orchestration use case.
        """
        self._db = db
        self._market_data_base_url = str(market_data_base_url or "").rstrip("/")
        self._sem = asyncio.Semaphore(int(max_concurrency))
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._signal_waker = signal_waker

    async def execute(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
    ) -> None:
        """
        Execute the trade evaluation flow for a closed candle event.
        """
        async with self._sem:
            try:
                await self._execute_inner(
                    stream_key=str(stream_key).strip().lower(),
                    ts=int(ts),
                    source=str(source).strip().lower(),
                    symbol=str(symbol).strip().upper(),
                    interval=str(interval).strip().lower(),
                )
            except Exception as exc:
                self._logger.exception(
                    "TradeCandleClosedTriggerUseCase failed. stream_key=%s ts=%s err=%s",
                    stream_key,
                    ts,
                    exc,
                )

    async def _execute_inner(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
    ) -> None:
        """
        Build dependencies and evaluate all active trade strategies for the stream.
        """
        strategy_repo = TradeStrategyRepositoryMongoDB(self._db)
        signal_repo = TradeSignalRepositoryMongoDB(self._db)
        runtime_snapshot_repo = TradeStrategyRuntimeSnapshotRepositoryMongoDB(self._db)

        market_data_client = MarketDataHttpClient(base_url=self._market_data_base_url)

        evaluator = EvaluateActiveTradeStrategiesUseCase(
            strategy_repo=strategy_repo,
            signal_repo=signal_repo,
            runtime_snapshot_repo=runtime_snapshot_repo,
            market_data_client=market_data_client,
            logger=self._logger,
        )

        signals = await evaluator.execute_for_stream(
            stream_key=stream_key,
            ts=ts,
            source=source,
            symbol=symbol,
            interval=interval,
        )

        if signals and self._signal_waker is not None:
            self._signal_waker()