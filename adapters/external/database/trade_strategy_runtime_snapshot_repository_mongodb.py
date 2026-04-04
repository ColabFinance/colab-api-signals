from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.trade_strategy_runtime_snapshot_entity import (
    TradeStrategyRuntimeSnapshotEntity,
)
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)


class TradeStrategyRuntimeSnapshotRepositoryMongoDB(TradeStrategyRuntimeSnapshotRepository):
    """
    MongoDB repository for the latest runtime state of each trade strategy.

    This collection stores exactly one current-state document per strategy_id.
    """

    COLLECTION = "trade_strategy_runtime_latest"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the repository with a MongoDB database handle.
        """
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Create indexes required for latest runtime state storage.
        """
        await self._col.create_index([("strategy_id", 1)], unique=True, name="ux_trade_runtime_latest_strategy")
        await self._col.create_index([("stream_key", 1), ("ts", -1)], name="ix_trade_runtime_latest_stream_ts")
        await self._col.create_index([("runtime_state", 1), ("ts", -1)], name="ix_trade_runtime_latest_state_ts")

    async def upsert(self, snapshot: TradeStrategyRuntimeSnapshotEntity) -> None:
        """
        Upsert the current runtime state by strategy_id.
        """
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        now_iso = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

        payload = snapshot.model_dump(
            mode="python",
            exclude={"id", "created_at", "created_at_iso", "updated_at", "updated_at_iso"},
        )
        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        await self._col.update_one(
            {"strategy_id": str(snapshot.strategy_id)},
            {
                "$set": payload,
                "$setOnInsert": {
                    "created_at": now_ms,
                    "created_at_iso": now_iso,
                },
            },
            upsert=True,
        )

    async def get_latest_by_strategy_id(
        self,
        strategy_id: str,
    ) -> Optional[TradeStrategyRuntimeSnapshotEntity]:
        """
        Return the latest runtime state for a strategy.
        """
        doc = await self._col.find_one({"strategy_id": str(strategy_id)})
        if not doc:
            return None
        return TradeStrategyRuntimeSnapshotEntity(**doc)