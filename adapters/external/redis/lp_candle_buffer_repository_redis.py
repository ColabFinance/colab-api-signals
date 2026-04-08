from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from redis.asyncio.client import Redis


class LpCandleBufferRepositoryRedis:
    """
    Redis-backed rolling candle buffer for LP evaluation.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        key_prefix: str,
        maxlen: int,
    ) -> None:
        self._redis = redis_client
        self._key_prefix = str(key_prefix).strip().rstrip(":")
        self._maxlen = int(maxlen)

    def _key(self, stream_key: str) -> str:
        return f"{self._key_prefix}:{str(stream_key).strip().lower()}"

    async def append_candle(
        self,
        *,
        stream_key: str,
        candle: Dict[str, Any],
        maxlen: Optional[int] = None,
    ) -> None:
        key = self._key(stream_key)
        effective_maxlen = int(maxlen or self._maxlen)

        await self._redis.rpush(key, json.dumps(candle))
        await self._redis.ltrim(key, -effective_maxlen, -1)

    async def replace_candles(
        self,
        *,
        stream_key: str,
        candles: List[Dict[str, Any]],
        maxlen: Optional[int] = None,
    ) -> None:
        key = self._key(stream_key)
        effective_maxlen = int(maxlen or self._maxlen)

        pipe = self._redis.pipeline()
        pipe.delete(key)
        if candles:
            trimmed = candles[-effective_maxlen:]
            pipe.rpush(key, *[json.dumps(item) for item in trimmed])
        await pipe.execute()

    async def list_candles(
        self,
        *,
        stream_key: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        key = self._key(stream_key)
        raw = await self._redis.lrange(key, -int(limit), -1)

        out: List[Dict[str, Any]] = []
        for item in raw:
            try:
                if isinstance(item, bytes):
                    item = item.decode("utf-8")
                out.append(json.loads(item))
            except Exception:
                continue

        out.sort(key=lambda x: int(x.get("close_time") or 0))
        return out