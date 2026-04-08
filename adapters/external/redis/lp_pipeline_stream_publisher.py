from __future__ import annotations

import json
import zlib
from typing import Any, Dict, Optional

from redis.asyncio.client import Redis


class LpPipelineStreamPublisher:
    """
    Publisher for LP candle evaluation events.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        eval_stream_prefix: str,
        eval_shard_count: int,
        stream_maxlen: int,
        logger=None,
    ) -> None:
        self._redis = redis_client
        self._eval_stream_prefix = str(eval_stream_prefix).strip().rstrip(":")
        self._eval_shard_count = max(1, int(eval_shard_count))
        self._stream_maxlen = int(stream_maxlen)
        self._logger = logger

    def build_eval_stream_name(self, shard_id: int) -> str:
        return f"{self._eval_stream_prefix}:{int(shard_id)}"

    def _resolve_shard(self, stream_key: str) -> int:
        return int(zlib.crc32(str(stream_key).strip().lower().encode("utf-8")) % self._eval_shard_count)

    async def publish_eval_event(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
        candle: Optional[Dict[str, Any]] = None,
    ) -> str:
        shard_id = self._resolve_shard(stream_key)
        stream_name = self.build_eval_stream_name(shard_id)

        payload = {
            "stream_key": str(stream_key).strip().lower(),
            "ts": str(int(ts)),
            "source": str(source).strip().lower(),
            "symbol": str(symbol).strip().upper(),
            "interval": str(interval).strip().lower(),
        }
        if candle is not None:
            payload["candle"] = json.dumps(candle)

        msg_id = await self._redis.xadd(
            stream_name,
            payload,
            maxlen=self._stream_maxlen,
            approximate=True,
        )
        return msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)