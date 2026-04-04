from __future__ import annotations

import json
from typing import Any, Dict, List

from redis.asyncio import Redis


class TradeCandleBufferRepositoryRedis:
    """
    Maintain a rolling hot buffer of trade candles in Redis lists.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        key_prefix: str,
        maxlen: int,
    ) -> None:
        """
        Initialize the Redis-backed candle buffer repository.
        """
        self._redis = redis_client
        self._key_prefix = str(key_prefix).strip().rstrip(":")
        self._maxlen = int(maxlen)

    async def upsert_candle(
        self,
        *,
        stream_key: str,
        candle: Dict[str, Any],
        maxlen: int | None = None,
    ) -> None:
        """
        Upsert a candle into the rolling buffer using open_time as the unique key.

        Fast path:
        - append when the incoming candle is newer than the current tail
        - replace tail when the incoming candle has the same open_time as the tail

        Slow path:
        - rebuild only when an out-of-order candle arrives
        """
        key = self._build_key(stream_key)
        resolved_maxlen = int(maxlen or self._maxlen)
        incoming_open_time = int(candle["open_time"])
        serialized = json.dumps(candle, separators=(",", ":"), sort_keys=True)

        tail_raw = await self._redis.lindex(key, -1)
        if tail_raw is None:
            pipe = self._redis.pipeline()
            pipe.rpush(key, serialized)
            pipe.ltrim(key, -resolved_maxlen, -1)
            await pipe.execute()
            return

        tail = json.loads(tail_raw)
        tail_open_time = int(tail["open_time"])

        if incoming_open_time > tail_open_time:
            pipe = self._redis.pipeline()
            pipe.rpush(key, serialized)
            pipe.ltrim(key, -resolved_maxlen, -1)
            await pipe.execute()
            return

        if incoming_open_time == tail_open_time:
            length = int(await self._redis.llen(key))
            if length <= 0:
                await self.replace(
                    stream_key=stream_key,
                    candles=[candle],
                    maxlen=resolved_maxlen,
                )
                return

            await self._redis.lset(key, length - 1, serialized)
            return

        await self._upsert_out_of_order(
            stream_key=stream_key,
            candle=candle,
            maxlen=resolved_maxlen,
        )

    async def list_last(self, *, stream_key: str, limit: int) -> List[Dict[str, Any]]:
        """
        Return the last N candles in ascending order.
        """
        if int(limit) <= 0:
            return []

        key = self._build_key(stream_key)
        items = await self._redis.lrange(key, -int(limit), -1)
        return [json.loads(item) for item in items]

    async def replace(
        self,
        *,
        stream_key: str,
        candles: List[Dict[str, Any]],
        maxlen: int | None = None,
    ) -> None:
        """
        Replace the full rolling buffer contents for a stream.
        """
        key = self._build_key(stream_key)
        resolved_maxlen = int(maxlen or self._maxlen)
        serialized = [
            json.dumps(candle, separators=(",", ":"), sort_keys=True)
            for candle in candles[-resolved_maxlen:]
        ]

        pipe = self._redis.pipeline()
        pipe.delete(key)

        if serialized:
            pipe.rpush(key, *serialized)
            pipe.ltrim(key, -resolved_maxlen, -1)

        await pipe.execute()

    async def get_length(self, *, stream_key: str) -> int:
        """
        Return the current candle count stored for a stream buffer.
        """
        return int(await self._redis.llen(self._build_key(stream_key)))

    async def list_streams(self, *, limit: int = 500) -> List[Dict[str, Any]]:
        """
        List Redis candle-buffer keys along with their stream identifiers and lengths.
        """
        pattern = f"{self._key_prefix}:*"
        cursor = 0
        keys: List[str] = []

        while True:
            cursor, batch = await self._redis.scan(
                cursor=cursor,
                match=pattern,
                count=max(100, int(limit)),
            )
            keys.extend(batch)

            if cursor == 0 or len(keys) >= int(limit):
                break

        keys = keys[: int(limit)]
        if not keys:
            return []

        pipe = self._redis.pipeline()
        for key in keys:
            pipe.llen(key)

        lengths = await pipe.execute()

        out: List[Dict[str, Any]] = []
        for key, length in zip(keys, lengths):
            stream_key = key.removeprefix(f"{self._key_prefix}:")
            out.append(
                {
                    "redis_key": key,
                    "stream_key": stream_key,
                    "candle_count": int(length or 0),
                }
            )

        out.sort(key=lambda x: x["stream_key"])
        return out

    async def _upsert_out_of_order(
        self,
        *,
        stream_key: str,
        candle: Dict[str, Any],
        maxlen: int,
    ) -> None:
        """
        Rebuild the rolling buffer when an out-of-order candle arrives.

        This path should be rare and exists mainly for replays or recovery flows.
        """
        existing = await self.list_last(
            stream_key=stream_key,
            limit=maxlen,
        )

        merged_by_open_time: Dict[int, Dict[str, Any]] = {}
        for item in existing:
            merged_by_open_time[int(item["open_time"])] = item

        merged_by_open_time[int(candle["open_time"])] = candle

        merged = sorted(merged_by_open_time.values(), key=lambda x: int(x["open_time"]))
        await self.replace(
            stream_key=stream_key,
            candles=merged[-maxlen:],
            maxlen=maxlen,
        )

    def _build_key(self, stream_key: str) -> str:
        """
        Build the Redis key used for a stream candle buffer.
        """
        normalized_stream_key = str(stream_key).strip().lower()
        return f"{self._key_prefix}:{normalized_stream_key}"