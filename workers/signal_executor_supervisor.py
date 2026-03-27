from __future__ import annotations

import asyncio
import contextlib
import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from adapters.external.database.signal_repository_mongodb import SignalRepositoryMongoDB
from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB
from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from adapters.external.database.trade_signal_repository_mongodb import TradeSignalRepositoryMongoDB
from adapters.external.database.trade_strategy_repository_mongodb import TradeStrategyRepositoryMongoDB
from adapters.external.database.trade_strategy_runtime_snapshot_repository_mongodb import (
    TradeStrategyRuntimeSnapshotRepositoryMongoDB,
)
from adapters.external.database.trade_trigger_event_repository_mongodb import (
    TradeTriggerEventRepositoryMongoDB,
)
from adapters.external.database.trigger_event_repository_mongodb import TriggerEventRepositoryMongoDB
from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.notify.telegram_notifier import TelegramNotifier
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from adapters.external.trade_execution.trade_execution_http_client import (
    TradeExecutionHttpClient,
)
from core.usecases.execute_signal_pipeline_use_case import ExecuteSignalPipelineUseCase
from core.usecases.execute_trade_signal_pipeline_use_case import (
    ExecuteTradeSignalPipelineUseCase,
)


class SignalExecutorSupervisor:
    """
    Background supervisor responsible for:

    - ensuring Mongo indexes for LP and trade flows
    - running the LP signal execution loop
    - running the trade signal execution loop

    The same wake mechanism is shared by both loops so that newly created
    LP signals and newly created trade signals are drained quickly.
    """

    def __init__(
        self,
        *,
        mongo_client: AsyncIOMotorClient,
        db: AsyncIOMotorDatabase,
        lp_base_url: str,
        trade_execution_base_url: str,
        telegram_bot_token: str = "",
        telegram_chat_id: str = "",
        poll_interval_s: float = 5.0,
    ) -> None:
        """
        Initialize the supervisor.
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_client = mongo_client
        self._db = db
        self._lp_base_url = str(lp_base_url or "").rstrip("/")
        self._trade_execution_base_url = str(trade_execution_base_url or "").rstrip("/")
        self._telegram_bot_token = telegram_bot_token or ""
        self._telegram_chat_id = telegram_chat_id or ""
        self._poll_interval_s = float(poll_interval_s)

        self._task: asyncio.Task | None = None
        self._lp_uc: ExecuteSignalPipelineUseCase | None = None
        self._trade_uc: ExecuteTradeSignalPipelineUseCase | None = None
        self._lp_client: PipelineHttpClient | None = None
        self._trade_execution_client: TradeExecutionHttpClient | None = None

        self._wake = asyncio.Event()

    def wake(self) -> None:
        """
        Wake the background loop because new work may be available.
        """
        self._wake.set()

    async def start(self) -> None:
        """
        Ensure indexes and start the background processing loop.
        """
        strategy_repo = StrategyRepositoryMongoDB(self._db)
        episode_repo = StrategyEpisodeRepositoryMongoDB(self._db)
        signal_repo = SignalRepositoryMongoDB(self._db)
        trigger_repo = TriggerEventRepositoryMongoDB(self._db)

        trade_strategy_repo = TradeStrategyRepositoryMongoDB(self._db)
        trade_signal_repo = TradeSignalRepositoryMongoDB(self._db)
        trade_runtime_snapshot_repo = TradeStrategyRuntimeSnapshotRepositoryMongoDB(self._db)
        trade_trigger_repo = TradeTriggerEventRepositoryMongoDB(self._db)

        await strategy_repo.ensure_indexes()
        await episode_repo.ensure_indexes()
        await signal_repo.ensure_indexes()
        await trigger_repo.ensure_indexes()

        await trade_strategy_repo.ensure_indexes()
        await trade_signal_repo.ensure_indexes()
        await trade_runtime_snapshot_repo.ensure_indexes()
        await trade_trigger_repo.ensure_indexes()

        notifier = None
        if self._telegram_bot_token and self._telegram_chat_id:
            notifier = TelegramNotifier(
                bot_token=self._telegram_bot_token,
                chat_id=self._telegram_chat_id,
            )

        self._lp_client = PipelineHttpClient(self._lp_base_url)
        market_data_client = MarketDataHttpClient.from_settings()
        self._trade_execution_client = TradeExecutionHttpClient(base_url=self._trade_execution_base_url)

        self._lp_uc = ExecuteSignalPipelineUseCase(
            signal_repo=signal_repo,
            episode_repo=episode_repo,
            lp_client=self._lp_client,
            notifier=notifier,
        )

        self._trade_uc = ExecuteTradeSignalPipelineUseCase(
            trade_signal_repo=trade_signal_repo,
            runtime_snapshot_repo=trade_runtime_snapshot_repo,
            trade_execution_client=self._trade_execution_client,
            market_data_client=market_data_client,
            notifier=notifier,
        )

        async def _loop() -> None:
            """
            Internal forever loop for LP and trade signal execution.
            """
            self._logger.info("Signal executor loop started. poll_interval_s=%s", self._poll_interval_s)
            while True:
                processed_any = False
                try:
                    lp_processed = await self._lp_uc.execute_once()
                    trade_processed = await self._trade_uc.execute_once()
                    processed_any = bool(lp_processed or trade_processed)
                except Exception as exc:
                    self._logger.exception("Signal executor loop error: %s", exc)

                if processed_any:
                    continue

                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval_s)
                except asyncio.TimeoutError:
                    pass
                finally:
                    self._wake.clear()

        if self._task is None or self._task.done():
            self._task = asyncio.create_task(_loop())

    async def stop(self) -> None:
        """
        Stop the background loop and close HTTP clients.
        """
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
            self._task = None

        if self._lp_client:
            with contextlib.suppress(Exception):
                await self._lp_client.aclose()
            self._lp_client = None

        if self._trade_execution_client:
            with contextlib.suppress(Exception):
                await self._trade_execution_client.aclose()
            self._trade_execution_client = None

        self._lp_uc = None
        self._trade_uc = None
        self._logger.info("Signal executor loop stopped.")