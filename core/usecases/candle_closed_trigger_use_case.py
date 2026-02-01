from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional

from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.external.database.signal_repository_mongodb import SignalRepositoryMongoDB
from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB
from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from core.services.strategy_reconciler_service import StrategyReconcilerService
from core.usecases.evaluate_active_strategies_use_case import EvaluateActiveStrategiesUseCase


class CandleClosedTriggerUseCase:
    """
    Orchestrates candle-closed trigger processing.

    - Optionally fetches missing indicator_set / indicator_snapshot from api-market-data.
    - Validates required fields.
    - Executes EvaluateActiveStrategiesUseCase.
    - Runs with bounded concurrency (semaphore) for safety.
    """

    def __init__(
        self,
        *,
        db: AsyncIOMotorDatabase,
        market_data_base_url: str,
        pipeline_base_url: str,
        max_concurrency: int = 10,
        logger: logging.Logger | None = None,
        signal_waker: Optional[Callable[[], None]] = None,
    ):
        self._db = db
        self._market_data_base_url = str(market_data_base_url or "").rstrip("/")
        self._pipeline_base_url = str(pipeline_base_url or "").rstrip("/")
        self._sem = asyncio.Semaphore(int(max_concurrency))
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._signal_waker = signal_waker
        
    @staticmethod
    def _require_snapshot_fields(indicator_snapshot: Dict[str, Any]) -> None:
        required = ["symbol", "close", "ema_fast", "ema_slow", "atr_pct", "ts"]
        for k in required:
            if k not in indicator_snapshot:
                raise ValueError(f"indicator_snapshot missing required field: {k}")

    async def execute(
        self,
        *,
        indicator_set_id: str,
        ts: int,
        indicator_set: Optional[Dict[str, Any]] = None,
        indicator_snapshot: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._sem:
            try:
                await self._execute_inner(
                    indicator_set_id=indicator_set_id,
                    ts=int(ts),
                    indicator_set=indicator_set,
                    indicator_snapshot=indicator_snapshot,
                )
            except Exception as exc:
                self._logger.exception(
                    "CandleClosedTriggerUseCase failed. indicator_set_id=%s ts=%s err=%s",
                    indicator_set_id,
                    ts,
                    exc,
                )

    async def _execute_inner(
        self,
        *,
        indicator_set_id: str,
        ts: int,
        indicator_set: Optional[Dict[str, Any]],
        indicator_snapshot: Optional[Dict[str, Any]],
    ) -> None:

        if indicator_set is None:
            self._logger.error(
                "indicator_set missing and MARKET_DATA_BASE_URL not configured. indicator_set_id=%s ts=%s",
                indicator_set_id,
                ts,
            )
            return

        if indicator_snapshot is None:
            self._logger.error(
                "indicator_snapshot missing and MARKET_DATA_BASE_URL not configured. indicator_set_id=%s ts=%s",
                indicator_set_id,
                ts,
            )
            return

        # Validate snapshot
        try:
            self._require_snapshot_fields(indicator_snapshot)
        except ValueError as exc:
            self._logger.error(
                "Invalid indicator_snapshot. indicator_set_id=%s ts=%s err=%s",
                indicator_set_id,
                ts,
                exc,
            )
            return

        # Build UC dependencies
        strategy_repo = StrategyRepositoryMongoDB(self._db)
        episode_repo = StrategyEpisodeRepositoryMongoDB(self._db)
        signal_repo = SignalRepositoryMongoDB(self._db)

        lp_client = PipelineHttpClient(base_url=self._pipeline_base_url) if self._pipeline_base_url else PipelineHttpClient(base_url="")

        reconciler = StrategyReconcilerService(lp_client=lp_client)

        eval_uc = EvaluateActiveStrategiesUseCase(
            strategy_repo=strategy_repo,
            episode_repo=episode_repo,
            signal_repo=signal_repo,
            reconciling_service=reconciler,
            lp_client=lp_client,
            logger=self._logger,
            on_signal_created=self._signal_waker,
        )

        await eval_uc.execute_for_indicator_snapshot(
            indicator_set=indicator_set,
            indicator_snapshot=indicator_snapshot,
        )
