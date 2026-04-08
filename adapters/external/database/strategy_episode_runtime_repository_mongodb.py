from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.strategy_episode_runtime_entity import StrategyEpisodeRuntimeEntity
from core.repositories.strategy_episode_runtime_repository import StrategyEpisodeRuntimeRepository


class StrategyEpisodeRuntimeRepositoryMongoDB(StrategyEpisodeRuntimeRepository):
    COLLECTION = "strategy_episode_runtimes"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._ensure_index(
            [("episode_id", 1), ("ts", 1)],
            unique=True,
            name="ux_episode_runtime_episode_ts",
        )
        await self._ensure_index(
            [("episode_id", 1), ("ts", -1)],
            name="ix_episode_runtime_episode_ts_desc",
        )
        await self._ensure_index(
            [("strategy_id", 1), ("ts", -1)],
            name="ix_episode_runtime_strategy_ts_desc",
        )
        await self._ensure_index(
            [("created_at", -1)],
            name="ix_episode_runtime_created_at",
        )

    async def _ensure_index(
        self,
        keys: Sequence[Tuple[str, int]],
        *,
        name: str,
        unique: bool = False,
    ) -> None:
        info = await self._col.index_information()

        desired_keys = tuple((str(field), int(direction)) for field, direction in keys)
        desired_unique = bool(unique)

        for _, existing in info.items():
            existing_keys = tuple(
                (str(field), int(direction))
                for field, direction in (existing.get("key") or [])
            )
            existing_unique = bool(existing.get("unique", False))

            if existing_keys == desired_keys and existing_unique == desired_unique:
                return

        existing_same_name = info.get(name)
        if existing_same_name is not None:
            existing_keys = tuple(
                (str(field), int(direction))
                for field, direction in (existing_same_name.get("key") or [])
            )
            existing_unique = bool(existing_same_name.get("unique", False))

            if existing_keys != desired_keys or existing_unique != desired_unique:
                await self._col.drop_index(name)

        await self._col.create_index(
            list(keys),
            unique=desired_unique,
            name=name,
        )

    def _now(self) -> tuple[int, str]:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        return now_ms, now_iso

    async def upsert(self, runtime: StrategyEpisodeRuntimeEntity) -> StrategyEpisodeRuntimeEntity:
        now_ms, now_iso = self._now()

        payload = runtime.to_mongo()
        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        await self._col.update_one(
            {"episode_id": str(runtime.episode_id), "ts": int(runtime.ts)},
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
            {"episode_id": str(runtime.episode_id), "ts": int(runtime.ts)}
        )
        return StrategyEpisodeRuntimeEntity.from_mongo(stored)

    async def list_by_episode(
        self,
        *,
        episode_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[StrategyEpisodeRuntimeEntity | None]:
        cursor = (
            self._col.find({"episode_id": str(episode_id)})
            .sort([("ts", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        return [StrategyEpisodeRuntimeEntity.from_mongo(doc) for doc in docs if doc]

    async def count_by_episode(self, *, episode_id: str) -> int:
        return int(await self._col.count_documents({"episode_id": str(episode_id)}))

    async def list_by_strategy(
        self,
        *,
        strategy_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[StrategyEpisodeRuntimeEntity | None]:
        cursor = (
            self._col.find({"strategy_id": str(strategy_id)})
            .sort([("ts", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        return [StrategyEpisodeRuntimeEntity.from_mongo(doc) for doc in docs if doc]

    async def count_by_strategy(self, *, strategy_id: str) -> int:
        return int(await self._col.count_documents({"strategy_id": str(strategy_id)}))

    async def get_latest_by_strategy(
        self,
        *,
        strategy_id: str,
    ) -> Optional[StrategyEpisodeRuntimeEntity]:
        doc = await self._col.find_one(
            {"strategy_id": str(strategy_id)},
            sort=[("ts", -1), ("created_at", -1)],
        )
        if not doc:
            return None
        return StrategyEpisodeRuntimeEntity.from_mongo(doc)