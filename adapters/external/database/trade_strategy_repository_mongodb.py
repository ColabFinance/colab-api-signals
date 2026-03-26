from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
from core.repositories.trade_strategy_repository import TradeStrategyRepository


class TradeStrategyRepositoryMongoDB(TradeStrategyRepository):
    """
    MongoDB repository for trade strategies.

    Trade strategies are stored separately from LP strategies to keep both
    evaluation pipelines isolated.
    """

    COLLECTION = "trade_strategies"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the repository with a MongoDB database handle.
        """
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Create indexes required for efficient strategy lookup by stream_key and status.
        """
        await self._col.create_index([("stream_key", 1), ("status", 1)], name="ix_stream_status")
        await self._col.create_index([("symbol", 1), ("interval", 1), ("status", 1)], name="ix_symbol_interval_status")
        await self._col.create_index([("name", 1)], name="ix_name")

    def _now(self) -> tuple[int, str]:
        """
        Return current UTC time as milliseconds and ISO string.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return now_ms, now_iso

    async def create(self, strategy: TradeStrategyEntity) -> TradeStrategyEntity:
        """
        Insert a new trade strategy and return the stored document.
        """
        now_ms, now_iso = self._now()

        doc = strategy.to_mongo()
        doc["created_at"] = now_ms
        doc["created_at_iso"] = now_iso
        doc["updated_at"] = now_ms
        doc["updated_at_iso"] = now_iso

        res = await self._col.insert_one(doc)
        stored = await self._col.find_one({"_id": res.inserted_id})
        return TradeStrategyEntity.from_mongo(stored)

    async def get_by_id(self, strategy_id: str) -> Optional[TradeStrategyEntity]:
        """
        Fetch a trade strategy by Mongo identifier.
        """
        doc = await self._col.find_one({"_id": strategy_id})
        if doc:
            return TradeStrategyEntity.from_mongo(doc)

        try:
            from bson import ObjectId
            doc = await self._col.find_one({"_id": ObjectId(strategy_id)})
            return TradeStrategyEntity.from_mongo(doc) if doc else None
        except Exception:
            return None

    async def list(
        self,
        *,
        stream_key: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 500,
    ) -> List[TradeStrategyEntity]:
        """
        List strategies with optional stream_key and status filters.
        """
        query: dict = {}
        if stream_key:
            query["stream_key"] = str(stream_key).strip().lower()
        if status:
            query["status"] = str(status).strip().upper()

        cursor = self._col.find(query).sort([("updated_at", -1), ("created_at", -1)]).limit(int(limit))
        docs = await cursor.to_list(length=int(limit))
        return [TradeStrategyEntity.from_mongo(doc) for doc in docs if doc]

    async def get_active_by_stream_key(self, stream_key: str) -> List[TradeStrategyEntity]:
        """
        Fetch all active strategies for a stream_key.
        """
        cursor = self._col.find(
            {
                "stream_key": str(stream_key).strip().lower(),
                "status": "ACTIVE",
            }
        )
        docs = await cursor.to_list(length=None)
        return [TradeStrategyEntity.from_mongo(doc) for doc in docs if doc]

    async def set_status(self, strategy_id: str, status: str) -> Optional[TradeStrategyEntity]:
        """
        Update strategy status and return the updated document.
        """
        now_ms, now_iso = self._now()

        updated = None
        await self._col.update_one(
            {"_id": strategy_id},
            {"$set": {"status": str(status).strip().upper(), "updated_at": now_ms, "updated_at_iso": now_iso}},
        )
        updated = await self.get_by_id(strategy_id)
        if updated is not None:
            return updated

        try:
            from bson import ObjectId
            await self._col.update_one(
                {"_id": ObjectId(strategy_id)},
                {"$set": {"status": str(status).strip().upper(), "updated_at": now_ms, "updated_at_iso": now_iso}},
            )
            return await self.get_by_id(strategy_id)
        except Exception:
            return None