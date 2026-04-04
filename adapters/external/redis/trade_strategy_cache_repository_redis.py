from __future__ import annotations

import json
from typing import List, Optional

from redis.asyncio import Redis

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity


class TradeStrategyCacheRepositoryRedis:
    """
    Cache active trade strategies by stream_key in Redis.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        key_prefix: str,
        ttl_s: int,
    ) -> None:
        """
        Initialize the Redis-backed strategy cache repository.
        """
        self._redis = redis_client
        self._key_prefix = str(key_prefix).strip().rstrip(":")
        self._ttl_s = int(ttl_s)

    async def get_active_by_stream_key(
        self,
        *,
        stream_key: str,
    ) -> Optional[List[TradeStrategyEntity]]:
        """
        Return cached active strategies for a stream_key when available.
        """
        key = self._build_key(stream_key)
        raw = await self._redis.get(key)
        if raw is None:
            return None

        items = json.loads(raw)
        return [TradeStrategyEntity(**item) for item in items]

    async def set_active_by_stream_key(
        self,
        *,
        stream_key: str,
        strategies: List[TradeStrategyEntity],
    ) -> None:
        """
        Cache active strategies for a stream_key.

        A long TTL is used by default, but active write paths also refresh the cache.
        When ttl_s <= 0, the cache is stored without expiration.
        """
        key = self._build_key(stream_key)
        payload = json.dumps(
            [strategy.model_dump(mode="json") for strategy in strategies],
            separators=(",", ":"),
            sort_keys=True,
        )

        if self._ttl_s > 0:
            await self._redis.set(key, payload, ex=self._ttl_s)
            return

        await self._redis.set(key, payload)

    async def invalidate(
        self,
        *,
        stream_key: str,
    ) -> None:
        """
        Invalidate the cached strategies for a stream_key.
        """
        await self._redis.delete(self._build_key(stream_key))

    async def get_ttl(
        self,
        *,
        stream_key: str,
    ) -> int:
        """
        Return the current Redis TTL for a stream strategy cache key.
        """
        return int(await self._redis.ttl(self._build_key(stream_key)))

    def _build_key(self, stream_key: str) -> str:
        """
        Build the Redis key used for a stream strategy cache.
        """
        normalized_stream_key = str(stream_key).strip().lower()
        return f"{self._key_prefix}:{normalized_stream_key}"