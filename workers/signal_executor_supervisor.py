from __future__ import annotations

import asyncio
import contextlib
import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from adapters.external.database.signal_repository_mongodb import SignalRepositoryMongoDB
from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB
from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from adapters.external.database.trigger_event_repository_mongodb import TriggerEventRepositoryMongoDB
from adapters.external.notify.telegram_notifier import TelegramNotifier
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from core.usecases.execute_signal_pipeline_use_case import ExecuteSignalPipelineUseCase


class SignalExecutorSupervisor:
    """
    Background supervisor responsible for the LP signal execution loop only.

    Trade execution now belongs to a dedicated Redis-based pipeline.
    """

    def __init__(
        self,
        *,
        mongo_client: AsyncIOMotorClient,
        db: AsyncIOMotorDatabase,
        lp_base_url: str,
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
        self._telegram_bot_token = telegram_bot_token or ""
        self._telegram_chat_id = telegram_chat_id or ""
        self._poll_interval_s = float(poll_interval_s)

        self._task: asyncio.Task | None = None
        self._lp_uc: ExecuteSignalPipelineUseCase | None = None
        self._lp_client: PipelineHttpClient | None = None

        self._wake = asyncio.Event()

    def wake(self) -> None:
        """
        Wake the background loop because new LP work may be available.
        """
        self._wake.set()

    async def start(self) -> None:
        """
        Ensure LP indexes and start the background processing loop.
        """
        strategy_repo = StrategyRepositoryMongoDB(self._db)
        episode_repo = StrategyEpisodeRepositoryMongoDB(self._db)
        signal_repo = SignalRepositoryMongoDB(self._db)
        trigger_repo = TriggerEventRepositoryMongoDB(self._db)

        await strategy_repo.ensure_indexes()
        await episode_repo.ensure_indexes()
        await signal_repo.ensure_indexes()
        await trigger_repo.ensure_indexes()

        notifier = None
        if self._telegram_bot_token and self._telegram_chat_id:
            notifier = TelegramNotifier(
                bot_token=self._telegram_bot_token,
                chat_id=self._telegram_chat_id,
            )

        self._lp_client = PipelineHttpClient(self._lp_base_url)

        self._lp_uc = ExecuteSignalPipelineUseCase(
            signal_repo=signal_repo,
            episode_repo=episode_repo,
            lp_client=self._lp_client,
            notifier=notifier,
        )

        async def _loop() -> None:
            """
            Internal forever loop for LP signal execution.
            """
            self._logger.info("LP signal executor loop started. poll_interval_s=%s", self._poll_interval_s)
            while True:
                processed_any = False
                try:
                    processed_any = bool(await self._lp_uc.execute_once())
                except Exception as exc:
                    self._logger.exception("LP signal executor loop error: %s", exc)

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

        self._lp_uc = None
        self._logger.info("LP signal executor loop stopped.")