from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError

from core.domain.entities.trade_signal_entity import TradeSignalEntity
from core.domain.enums.trade_enums import TradeSignalStatus
from core.repositories.trade_signal_repository import TradeSignalRepository


class TradeSignalRepositoryMongoDB(TradeSignalRepository):
    """
    MongoDB repository for generated trade signals.

    Signals are inserted once by idempotency key and then updated only by
    the execution pipeline.
    """

    COLLECTION = "trade_signals"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Create indexes required for idempotent signal generation and execution.
        """
        await self._col.create_index([("idempotency_key", 1)], unique=True, name="ux_trade_signal_idempotency")
        await self._col.create_index([("strategy_id", 1), ("ts", -1)], name="ix_trade_signal_strategy_ts")
        await self._col.create_index([("stream_key", 1), ("ts", -1)], name="ix_trade_signal_stream_ts")
        await self._col.create_index([("status", 1), ("ts", -1)], name="ix_trade_signal_status_ts")

    def _now(self) -> tuple[int, str]:
        """
        Return current UTC time as milliseconds and ISO string.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return now_ms, now_iso

    async def upsert_by_idempotency_key(self, signal: TradeSignalEntity) -> TradeSignalEntity:
        """
        Insert a new signal if the idempotency key is still unseen.

        If the signal already exists, the existing stored document is returned
        unchanged so a previously SENT or FAILED signal is never reverted back
        to PENDING by accident.
        """
        existing = await self._col.find_one({"idempotency_key": signal.idempotency_key})
        if existing is not None:
            return TradeSignalEntity.from_mongo(existing)

        now_ms, now_iso = self._now()
        payload = signal.to_mongo()
        payload["created_at"] = now_ms
        payload["created_at_iso"] = now_iso
        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        try:
            await self._col.insert_one(payload)
        except DuplicateKeyError:
            existing = await self._col.find_one({"idempotency_key": signal.idempotency_key})
            if existing is not None:
                return TradeSignalEntity.from_mongo(existing)
            raise

        stored = await self._col.find_one({"idempotency_key": signal.idempotency_key})
        return TradeSignalEntity.from_mongo(stored)

    async def list(
        self,
        *,
        strategy_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[TradeSignalEntity]:
        """
        List stored trade signals, optionally filtered by strategy_id.
        """
        query: dict = {}
        if strategy_id:
            query["strategy_id"] = strategy_id

        cursor = self._col.find(query).sort([("ts", -1), ("created_at", -1)]).limit(int(limit))
        docs = await cursor.to_list(length=int(limit))
        return [TradeSignalEntity.from_mongo(doc) for doc in docs if doc]

    async def list_pending(self, limit: int) -> List[TradeSignalEntity]:
        """
        List pending trade signals.
        """
        cursor = (
            self._col.find({"status": TradeSignalStatus.PENDING.value})
            .sort([("ts", 1), ("created_at", 1)])
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        return [TradeSignalEntity.from_mongo(doc) for doc in docs if doc]

    async def mark_success(
        self,
        signal: TradeSignalEntity,
        execution_response: Optional[dict] = None,
    ) -> None:
        """
        Mark a trade signal as sent.
        """
        now_ms, now_iso = self._now()
        await self._col.update_one(
            {"idempotency_key": signal.idempotency_key},
            {
                "$set": {
                    "status": TradeSignalStatus.SENT.value,
                    "execution_response": execution_response,
                    "last_error": None,
                    "updated_at": now_ms,
                    "updated_at_iso": now_iso,
                },
                "$inc": {
                    "attempts": 1,
                },
            },
        )

    async def mark_failure(
        self,
        signal: TradeSignalEntity,
        error_message: str,
    ) -> None:
        """
        Mark a trade signal as failed and increment attempts.
        """
        now_ms, now_iso = self._now()
        await self._col.update_one(
            {"idempotency_key": signal.idempotency_key},
            {
                "$set": {
                    "status": TradeSignalStatus.FAILED.value,
                    "last_error": str(error_message),
                    "updated_at": now_ms,
                    "updated_at_iso": now_iso,
                },
                "$inc": {
                    "attempts": 1,
                },
            },
        )
    
    async def list(
        self,
        *,
        strategy_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[TradeSignalEntity]:
        """
        List generated trade signals ordered by newest first.
        """
        return await self.list_paginated(
            strategy_id=strategy_id,
            limit=int(limit),
            offset=0,
        )

    async def list_paginated(
        self,
        *,
        strategy_id: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> List[TradeSignalEntity]:
        """
        List generated trade signals with pagination support ordered by newest first.
        """
        query: dict = {}

        if strategy_id:
            query["strategy_id"] = str(strategy_id)

        cursor = (
            self._col.find(query)
            .sort([("ts", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )

        docs = await cursor.to_list(length=int(limit))
        return [TradeSignalEntity.from_mongo(doc) for doc in docs if doc]

    async def count(
        self,
        *,
        strategy_id: Optional[str] = None,
    ) -> int:
        """
        Count generated trade signals for pagination.
        """
        query: dict = {}

        if strategy_id:
            query["strategy_id"] = str(strategy_id)

        return int(await self._col.count_documents(query))