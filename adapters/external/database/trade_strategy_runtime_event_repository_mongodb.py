from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.trade_strategy_runtime_event_entity import (
    TradeStrategyRuntimeEventEntity,
)
from core.repositories.trade_strategy_runtime_event_repository import (
    TradeStrategyRuntimeEventRepository,
)


class TradeStrategyRuntimeEventRepositoryMongoDB(TradeStrategyRuntimeEventRepository):
    """
    MongoDB repository for trade strategy runtime event history.
    """

    COLLECTION = "trade_strategy_runtime_events"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the repository with a MongoDB database handle.
        """
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Create indexes required for runtime event history.
        """
        await self._col.create_index([("idempotency_key", 1)], unique=True, name="ux_runtime_event_idempotency")
        await self._col.create_index([("strategy_id", 1), ("ts", -1)], name="ix_runtime_event_strategy_ts")
        await self._col.create_index([("stream_key", 1), ("ts", -1)], name="ix_runtime_event_stream_ts")
        await self._col.create_index([("event", 1), ("ts", -1)], name="ix_runtime_event_type_ts")
        await self._col.create_index([("signal_type", 1), ("ts", -1)], name="ix_runtime_event_signal_ts")

    async def upsert(self, event: TradeStrategyRuntimeEventEntity) -> None:
        """
        Persist a runtime event using an idempotent key.
        """
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        now_iso = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

        payload = event.model_dump(
            mode="python",
            exclude={"id", "created_at", "created_at_iso", "updated_at", "updated_at_iso"},
        )
        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        await self._col.update_one(
            {"idempotency_key": str(event.idempotency_key)},
            {
                "$set": payload,
                "$setOnInsert": {
                    "created_at": now_ms,
                    "created_at_iso": now_iso,
                },
            },
            upsert=True,
        )

    async def list_last_by_strategy_id(
        self,
        strategy_id: str,
        limit: int,
    ) -> List[TradeStrategyRuntimeEventEntity]:
        """
        List the most recent runtime events for a strategy in ascending order.
        """
        cursor = (
            self._col.find({"strategy_id": str(strategy_id)})
            .sort("ts", -1)
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        entities = [TradeStrategyRuntimeEventEntity(**doc) for doc in docs]
        entities.reverse()
        return entities