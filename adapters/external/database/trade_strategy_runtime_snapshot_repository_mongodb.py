from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.trade_strategy_runtime_snapshot_entity import (
    TradeStrategyRuntimeSnapshotEntity,
)
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)


class TradeStrategyRuntimeSnapshotRepositoryMongoDB(TradeStrategyRuntimeSnapshotRepository):
    """
    MongoDB repository for trade strategy runtime snapshots.

    Runtime snapshots are stored separately from trade signals because they
    represent the computed internal state of the strategy at each closed candle,
    regardless of whether a signal was emitted or not.
    """

    COLLECTION = "trade_strategy_runtime_snapshots"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the repository with a MongoDB database handle.
        """
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Ensure indexes for idempotent storage and efficient latest/history reads.
        """
        await self._col.create_index(
            [("strategy_id", 1), ("ts", 1)],
            unique=True,
            name="ux_trade_strategy_runtime_strategy_ts",
        )
        await self._col.create_index(
            [("strategy_id", 1), ("ts", -1)],
            name="ix_trade_strategy_runtime_strategy_ts_desc",
        )
        await self._col.create_index(
            [("stream_key", 1), ("ts", -1)],
            name="ix_trade_strategy_runtime_stream_ts_desc",
        )
        await self._col.create_index(
            [("created_at", -1)],
            name="ix_trade_strategy_runtime_created_at",
        )

    def _now(self) -> tuple[int, str]:
        """
        Return current UTC time as milliseconds and ISO string.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return now_ms, now_iso

    async def upsert(self, snapshot: TradeStrategyRuntimeSnapshotEntity) -> TradeStrategyRuntimeSnapshotEntity:
        """
        Upsert a runtime snapshot keyed by strategy_id and ts.

        This guarantees one snapshot per strategy per closed candle.
        """
        now_ms, now_iso = self._now()

        payload = snapshot.to_mongo()
        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        await self._col.update_one(
            {"strategy_id": snapshot.strategy_id, "ts": int(snapshot.ts)},
            {
                "$set": payload,
                "$setOnInsert": {
                    "created_at": now_ms,
                    "created_at_iso": now_iso,
                },
            },
            upsert=True,
        )

        stored = await self._col.find_one(
            {"strategy_id": snapshot.strategy_id, "ts": int(snapshot.ts)}
        )
        return TradeStrategyRuntimeSnapshotEntity.from_mongo(stored)

    async def get_latest_by_strategy_id(
        self,
        strategy_id: str,
    ) -> Optional[TradeStrategyRuntimeSnapshotEntity]:
        """
        Fetch the latest runtime snapshot for a strategy.
        """
        doc = await self._col.find_one(
            {"strategy_id": str(strategy_id)},
            sort=[("ts", -1), ("created_at", -1)],
        )
        if not doc:
            return None
        return TradeStrategyRuntimeSnapshotEntity.from_mongo(doc)

    async def list_by_strategy_id(
        self,
        strategy_id: str,
        limit: int,
    ) -> List[TradeStrategyRuntimeSnapshotEntity]:
        """
        List runtime snapshots for a strategy in reverse chronological order.
        """
        cursor = (
            self._col.find({"strategy_id": str(strategy_id)})
            .sort([("ts", -1), ("created_at", -1)])
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        return [TradeStrategyRuntimeSnapshotEntity.from_mongo(doc) for doc in docs if doc]