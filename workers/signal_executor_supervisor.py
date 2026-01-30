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
    Background supervisor responsible for:
    - Ensuring Mongo indexes for signal execution flow.
    - Running a forever-loop that drains PENDING signals and executes the pipeline.
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
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_client = mongo_client
        self._db = db
        self._lp_base_url = str(lp_base_url or "").rstrip("/")
        self._telegram_bot_token = telegram_bot_token or ""
        self._telegram_chat_id = telegram_chat_id or ""
        self._poll_interval_s = float(poll_interval_s)

        self._task: asyncio.Task | None = None
        self._uc: ExecuteSignalPipelineUseCase | None = None
        self._lp_client: PipelineHttpClient | None = None
        
        self._wake = asyncio.Event()
        
    def wake(self) -> None:
        # chamada rápida: “tem trabalho novo”
        self._wake.set()
        
    async def start(self) -> None:
        # repos
        strategy_repo = StrategyRepositoryMongoDB(self._db)
        episode_repo = StrategyEpisodeRepositoryMongoDB(self._db)
        signal_repo = SignalRepositoryMongoDB(self._db)
        trigger_repo = TriggerEventRepositoryMongoDB(self._db)

        # indexes (important)
        await strategy_repo.ensure_indexes()
        await episode_repo.ensure_indexes()
        await signal_repo.ensure_indexes()
        await trigger_repo.ensure_indexes()

        # notifier (optional)
        notifier = None
        if self._telegram_bot_token and self._telegram_chat_id:
            notifier = TelegramNotifier(
                bot_token=self._telegram_bot_token,
                chat_id=self._telegram_chat_id,
            )

        # lp client
        self._lp_client = PipelineHttpClient(self._lp_base_url)

        # UC that drains signals
        self._uc = ExecuteSignalPipelineUseCase(
            signal_repo=signal_repo,
            episode_repo=episode_repo,
            lp_client=self._lp_client,
            notifier=notifier,
        )

        async def _loop() -> None:
            self._logger.info("Signal executor loop started. poll_interval_s=%s", self._poll_interval_s)
            while True:
                processed_any = False
                try:
                    processed_any = await self._uc.execute_once()
                except Exception as exc:
                    self._logger.exception("Signal executor loop error: %s", exc)

                # se processou algo, tenta drenar mais imediatamente (sem esperar)
                if processed_any:
                    continue

                # sem trabalho: espera “wake” OU timeout de fallback
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval_s)
                except asyncio.TimeoutError:
                    pass
                finally:
                    self._wake.clear()

        if self._task is None or self._task.done():
            self._task = asyncio.create_task(_loop())
            
    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
            self._task = None

        if self._lp_client:
            with contextlib.suppress(Exception):
                await self._lp_client.aclose()
            self._lp_client = None

        self._uc = None
        self._logger.info("Signal executor loop stopped.")