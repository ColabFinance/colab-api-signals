from __future__ import annotations

import time
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError

from core.repositories.trade_trigger_event_repository import TradeTriggerEventRepository


class TradeTriggerEventRepositoryMongoDB(TradeTriggerEventRepository):
    """
    MongoDB repository used to guarantee idempotency for trade candle-closed triggers.
    """

    COLLECTION = "trade_trigger_events"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the repository with a MongoDB database handle.
        """
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        """
        Create indexes required for idempotent processing of trade triggers.
        """
        await self._col.create_index([("stream_key", 1), ("ts", 1)], unique=True, name="ux_trade_stream_ts")
        await self._col.create_index([("created_at", -1)], name="ix_trade_created_at")

    async def mark_if_new(self, stream_key: str, ts: int) -> bool:
        """
        Insert a trigger marker if it does not already exist.

        Returns:
            True when the trigger event is new.
            False when it has already been processed before.
        """
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        doc = {
            "stream_key": str(stream_key).strip().lower(),
            "ts": int(ts),
            "created_at": now_ms,
            "created_at_iso": now_iso,
        }

        try:
            await self._col.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False