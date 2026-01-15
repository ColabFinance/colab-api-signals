import time
from datetime import datetime, timezone
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.strategy_entity import StrategyEntity
from core.repositories.strategy_repository import StrategyRepository


class StrategyRepositoryMongoDB(StrategyRepository):
    COLLECTION = "strategies"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._col.create_index([("status", 1), ("symbol", 1)], name="ix_status_symbol")
        await self._col.create_index([("indicator_set_id", 1), ("status", 1)], name="ix_set_status")
        await self._col.create_index([("name", 1), ("symbol", 1)], unique=True, name="ux_name_symbol")

        # onchain identity index (doesn't break old docs)
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

    def _now(self) -> tuple[int, str]:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return now_ms, now_iso

    async def upsert(self, strategy: StrategyEntity) -> StrategyEntity:
        now_ms, now_iso = self._now()

        doc = strategy.to_mongo()
        key = {"name": strategy.name, "symbol": strategy.symbol}
        update = {
            "$set": {
                **doc,
                "updated_at": now_ms,
                "updated_at_iso": now_iso,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        await self._col.update_one(key, update, upsert=True)
        found = await self._col.find_one(key)
        return StrategyEntity.from_mongo(found)

    async def upsert_by_onchain_identity(self, strategy: StrategyEntity) -> StrategyEntity:
        if not strategy.chain or not strategy.owner or not strategy.strategy_id:
            # fallback
            return await self.upsert(strategy)

        now_ms, now_iso = self._now()

        doc = strategy.to_mongo()
        key = {"chain": strategy.chain, "owner": strategy.owner, "strategy_id": int(strategy.strategy_id)}
        update = {
            "$set": {
                **doc,
                "updated_at": now_ms,
                "updated_at_iso": now_iso,
            },
            "$setOnInsert": {
                "created_at": now_ms,
                "created_at_iso": now_iso,
            },
        }
        await self._col.update_one(key, update, upsert=True)
        found = await self._col.find_one(key)
        return StrategyEntity.from_mongo(found)

    async def get_by_onchain_identity(self, chain: str, owner: str, strategy_id: int) -> Optional[StrategyEntity]:
        doc = await self._col.find_one({"chain": chain, "owner": owner, "strategy_id": int(strategy_id)})
        return StrategyEntity.from_mongo(doc)

    async def get_active_by_indicator_set(self, indicator_set_id: str) -> List[StrategyEntity]:
        cursor = self._col.find({"indicator_set_id": indicator_set_id, "status": "ACTIVE"})
        docs = await cursor.to_list(length=None)
        return [StrategyEntity.from_mongo(d) for d in docs if d]
