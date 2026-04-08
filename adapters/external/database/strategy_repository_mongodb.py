import time
from datetime import datetime, timezone
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from pymongo.errors import DuplicateKeyError
from core.domain.entities.strategy_entity import StrategyEntity
from core.repositories.strategy_repository import StrategyRepository


class StrategyRepositoryMongoDB(StrategyRepository):
    COLLECTION = "strategies"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    def _clean_set(self, doc: dict) -> dict:
        # do not overwrite existing fields with None
        return {k: v for k, v in (doc or {}).items() if v is not None}

    def _strip_set_forbidden_fields(self, doc: dict) -> dict:
        # MongoDB forbids updating _id; created_* should be insert-only
        for k in ("_id", "created_at", "created_at_iso"):
            doc.pop(k, None)
        return doc
    
    async def ensure_indexes(self) -> None:
        await self._col.create_index([("status", 1), ("symbol", 1)], name="ix_status_symbol")
        await self._col.create_index([("indicator_set_id", 1), ("status", 1)], name="ix_set_status")
        await self._col.create_index([("stream_key", 1), ("status", 1)], name="ix_stream_key_status")
        await self._col.create_index([("name", 1), ("symbol", 1)], unique=True, name="ux_name_symbol")

        await self._col.create_index(
            [("chain", 1), ("owner", 1), ("strategy_id", 1)],
            unique=True,
            name="ux_chain_owner_strategy_id",
            partialFilterExpression={
                "chain": {"$exists": True},
                "owner": {"$exists": True},
                "strategy_id": {"$exists": True},
            },
        )
        
        await self._col.create_index(
            [("chain", 1), ("status", 1), ("is_public", 1), ("updated_at", -1)],
            name="idx_strategy_public_explore",
        )

    def _now(self) -> tuple[int, str]:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return now_ms, now_iso

    async def upsert(self, strategy: StrategyEntity) -> StrategyEntity:
        now_ms, now_iso = self._now()

        doc = strategy.to_mongo()
        key = {"name": strategy.name, "symbol": strategy.symbol}
        
        set_doc = self._clean_set(doc)
        set_doc = self._strip_set_forbidden_fields(set_doc)
        
        update = {
            "$set": {
                **set_doc,
                "updated_at": now_ms,
                "updated_at_iso": now_iso,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        try:
            await self._col.update_one(key, update, upsert=True)
        except DuplicateKeyError as e:
            raise ValueError("DUPLICATE_NAME_SYMBOL") from e
        found = await self._col.find_one(key)
        return StrategyEntity.from_mongo(found)

    async def upsert_by_onchain_identity(self, strategy: StrategyEntity) -> StrategyEntity:
        if not strategy.chain or not strategy.owner or not strategy.strategy_id:
            return await self.upsert(strategy)

        now_ms, now_iso = self._now()

        doc = strategy.to_mongo()
        key = {"chain": strategy.chain, "owner": strategy.owner, "strategy_id": int(strategy.strategy_id)}
        
        set_doc = self._clean_set(doc)
        set_doc = self._strip_set_forbidden_fields(set_doc)
        
        update = {
            "$set": {
                **set_doc,
                "updated_at": now_ms,
                "updated_at_iso": now_iso,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        try:
            await self._col.update_one(key, update, upsert=True)
        except DuplicateKeyError as e:
            raise ValueError("DUPLICATE_NAME_SYMBOL") from e
        found = await self._col.find_one(key)
        return StrategyEntity.from_mongo(found)

    async def get_by_onchain_identity(self, chain: str, owner: str, strategy_id: int) -> Optional[StrategyEntity]:
        doc = await self._col.find_one({"chain": chain, "owner": owner, "strategy_id": int(strategy_id)})
        if not doc:
            return None
        return StrategyEntity.from_mongo(doc)

    async def get_active_by_indicator_set(self, indicator_set_id: str) -> List[StrategyEntity]:
        cursor = self._col.find({"indicator_set_id": indicator_set_id, "status": "ACTIVE"})
        docs = await cursor.to_list(length=None)
        return [StrategyEntity.from_mongo(d) for d in docs if d]

    async def get_active_by_stream_key(self, stream_key: str) -> List[StrategyEntity]:
        cursor = self._col.find(
            {
                "stream_key": str(stream_key).strip().lower(),
                "status": "ACTIVE",
            }
        )
        docs = await cursor.to_list(length=None)
        return [StrategyEntity.from_mongo(d) for d in docs if d]

    async def exists_by_name_symbol(self, name: str, symbol: str) -> bool:
        doc = await self._col.find_one({"name": name, "symbol": symbol}, {"_id": 1})
        return bool(doc)
    
    async def list_by_owner_chain(self, chain: str, owner: str, status: Optional[str] = None) -> List[StrategyEntity]:
        q: dict = {"chain": chain, "owner": owner}
        if status:
            q["status"] = status

        cursor = self._col.find(q).sort([("strategy_id", 1), ("created_at", 1)])
        docs = await cursor.to_list(length=None)
        return [StrategyEntity.from_mongo(d) for d in docs if d]
    
    async def list_public(
        self,
        *,
        chain: str | None = None,
        status: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[StrategyEntity]:
        query: dict = {"is_public": True}

        if chain:
            query["chain"] = chain
        if status:
            query["status"] = status

        cursor = (
            self._col.find(query)
            .sort([("updated_at", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )

        docs = await cursor.to_list(length=int(limit))
        return [StrategyEntity.from_mongo(doc) for doc in docs if doc]