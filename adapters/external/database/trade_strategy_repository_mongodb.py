from __future__ import annotations

import time
from bson import ObjectId
from datetime import datetime, timezone
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
from core.domain.enums.trade_enums import TradeStrategyStatus
from core.repositories.trade_strategy_repository import TradeStrategyRepository


class TradeStrategyRepositoryMongoDB(TradeStrategyRepository):
    """
    MongoDB repository for trade strategies.
    """

    COLLECTION = "trade_strategies"

    def __init__(self, db: AsyncIOMotorDatabase):
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

    def _normalize_status(self, status: TradeStrategyStatus | str | None) -> Optional[str]:
        if status is None:
            return None
        if isinstance(status, TradeStrategyStatus):
            return status.value
        return TradeStrategyStatus(str(status).strip().upper()).value

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
            query["status"] = self._normalize_status(status)

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
                "status": TradeStrategyStatus.ACTIVE.value,
            }
        )
        docs = await cursor.to_list(length=None)
        return [TradeStrategyEntity.from_mongo(doc) for doc in docs if doc]

    async def set_status(self, strategy_id: str, status: str) -> Optional[TradeStrategyEntity]:
        """
        Update strategy status and return the updated document.
        """
        now_ms, now_iso = self._now()
        normalized_status = self._normalize_status(status)

        await self._col.update_one(
            {"_id": strategy_id},
            {"$set": {"status": normalized_status, "updated_at": now_ms, "updated_at_iso": now_iso}},
        )
        updated = await self.get_by_id(strategy_id)
        if updated is not None:
            return updated

        try:
            from bson import ObjectId
            await self._col.update_one(
                {"_id": ObjectId(strategy_id)},
                {"$set": {"status": normalized_status, "updated_at": now_ms, "updated_at_iso": now_iso}},
            )
            return await self.get_by_id(strategy_id)
        except Exception:
            return None
    
    def _build_public_query(
        self,
        *,
        status: str | None = None,
        stream_key: str | None = None,
        symbol: str | None = None,
        execution_account_id: str | None = None,
        search: str | None = None,
    ) -> dict:
        """
        Build the public-facing query used by paginated trade strategy pages.
        """
        and_conditions: list[dict] = []

        if status:
            and_conditions.append({"status": str(status).strip().upper()})

        if stream_key:
            and_conditions.append({"stream_key": str(stream_key).strip().lower()})

        if symbol:
            and_conditions.append({"symbol": str(symbol).strip().upper()})

        if execution_account_id:
            and_conditions.append({"execution_account_id": str(execution_account_id).strip()})

        normalized_search = str(search).strip() if search else ""
        if normalized_search:
            or_conditions: list[dict] = [
                {"name": {"$regex": normalized_search, "$options": "i"}},
                {"symbol": {"$regex": normalized_search, "$options": "i"}},
                {"stream_key": {"$regex": normalized_search, "$options": "i"}},
                {"execution_account_id": {"$regex": normalized_search, "$options": "i"}},
            ]

            if ObjectId.is_valid(normalized_search):
                or_conditions.append({"_id": ObjectId(normalized_search)})

            and_conditions.append({"$or": or_conditions})

        if not and_conditions:
            return {}

        if len(and_conditions) == 1:
            return and_conditions[0]

        return {"$and": and_conditions}


    async def list_public(
        self,
        *,
        status: str | None = None,
        stream_key: str | None = None,
        symbol: str | None = None,
        execution_account_id: str | None = None,
        search: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[TradeStrategyEntity]: 
        """
        List trade strategies for public user-facing pages with filtering and pagination.
        """
        query = self._build_public_query(
            status=status,
            stream_key=stream_key,
            symbol=symbol,
            execution_account_id=execution_account_id,
            search=search,
        )

        cursor = (
            self._col.find(query)
            .sort([("updated_at", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )

        docs = await cursor.to_list(length=int(limit))
        return [TradeStrategyEntity.from_mongo(doc) for doc in docs if doc]


    async def count_public(
        self,
        *,
        status: str | None = None,
        stream_key: str | None = None,
        symbol: str | None = None,
        execution_account_id: str | None = None,
        search: str | None = None,
    ) -> int:
        """
        Count trade strategies for public user-facing pages with filtering.
        """
        query = self._build_public_query(
            status=status,
            stream_key=stream_key,
            symbol=symbol,
            execution_account_id=execution_account_id,
            search=search,
        )
        return int(await self._col.count_documents(query))


    async def get_public_summary(self) -> dict:
        """
        Return summary counters for public user-facing trade strategy pages.
        """
        total = int(await self._col.count_documents({}))
        active = int(await self._col.count_documents({"status": "ACTIVE"}))
        inactive = int(await self._col.count_documents({"status": "INACTIVE"}))

        stream_keys = await self._col.distinct(
            "stream_key",
            {"stream_key": {"$nin": [None, ""]}},
        )
        execution_account_ids = await self._col.distinct(
            "execution_account_id",
            {"execution_account_id": {"$nin": [None, ""]}},
        )

        return {
            "total": total,
            "active": active,
            "inactive": inactive,
            "unique_stream_keys": len([item for item in stream_keys if item]),
            "unique_execution_accounts": len([item for item in execution_account_ids if item]),
        }


    async def list_public_filter_options(self) -> dict:
        """
        Return available filter values for public user-facing trade strategy pages.
        """
        statuses = await self._col.distinct("status", {"status": {"$nin": [None, ""]}})
        stream_keys = await self._col.distinct("stream_key", {"stream_key": {"$nin": [None, ""]}})
        symbols = await self._col.distinct("symbol", {"symbol": {"$nin": [None, ""]}})
        execution_account_ids = await self._col.distinct(
            "execution_account_id",
            {"execution_account_id": {"$nin": [None, ""]}},
        )

        return {
            "statuses": sorted([str(item) for item in statuses if item]),
            "stream_keys": sorted([str(item) for item in stream_keys if item]),
            "symbols": sorted([str(item) for item in symbols if item]),
            "execution_account_ids": sorted([str(item) for item in execution_account_ids if item]),
        }