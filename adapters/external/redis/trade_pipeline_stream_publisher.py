from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Tuple

from redis.asyncio import Redis

from core.domain.entities.trade_signal_entity import TradeSignalEntity


class TradePipelineStreamPublisher:
    """
    Publish events between the internal trade pipelines.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        eval_stream_prefix: str,
        eval_shard_count: int,
        signal_stream_name: str,
        stream_maxlen: int,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the trade pipeline stream publisher.
        """
        self._redis = redis_client
        self._eval_stream_prefix = str(eval_stream_prefix).strip().rstrip(".")
        self._eval_shard_count = int(eval_shard_count)
        self._signal_stream_name = str(signal_stream_name).strip()
        self._stream_maxlen = int(stream_maxlen)
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    def resolve_shard(self, stream_key: str) -> int:
        """
        Resolve the logical shard for a stream_key using a stable hash.
        """
        normalized = str(stream_key).strip().lower().encode("utf-8")
        digest = hashlib.sha256(normalized).digest()
        shard = int.from_bytes(digest[:8], byteorder="big", signed=False) % self._eval_shard_count
        return int(shard)

    def build_eval_stream_name(self, shard_id: int) -> str:
        """
        Build the Redis Stream name for a given evaluation shard.
        """
        return f"{self._eval_stream_prefix}.{int(shard_id)}.v1"

    async def publish_candle_for_evaluation(
        self,
        *,
        stream_key: str,
        ts: int,
        source: str,
        symbol: str,
        interval: str,
        candle: Dict[str, Any],
    ) -> Tuple[int, str, str]:
        """
        Publish a closed candle event to the appropriate evaluation shard stream.
        """
        shard_id = self.resolve_shard(stream_key)
        stream_name = self.build_eval_stream_name(shard_id)

        payload = {
            "event_type": "trade.candle.eval",
            "event_version": "1",
            "stream_key": str(stream_key).strip().lower(),
            "ts": str(int(ts)),
            "source": str(source).strip().lower(),
            "symbol": str(symbol).strip().upper(),
            "interval": str(interval).strip().lower(),
            "shard_id": str(int(shard_id)),
            "candle_json": json.dumps(candle, separators=(",", ":"), sort_keys=True),
        }

        message_id = await self._redis.xadd(
            stream_name,
            payload,
            maxlen=self._stream_maxlen,
            approximate=True,
        )

        self._logger.debug(
            "Published candle to evaluation shard. stream_key=%s ts=%s shard=%s stream=%s message_id=%s",
            stream_key,
            ts,
            shard_id,
            stream_name,
            message_id,
        )
        return int(shard_id), stream_name, str(message_id)

    async def publish_generated_signal(
        self,
        *,
        signal: TradeSignalEntity,
    ) -> str:
        """
        Publish a generated trade signal to the execution stream.
        """
        payload = {
            "event_type": "trade.signal.generated",
            "event_version": "1",
            "strategy_id": str(signal.strategy_id),
            "stream_key": str(signal.stream_key).strip().lower(),
            "symbol": str(signal.symbol).strip().upper(),
            "interval": str(signal.interval).strip().lower(),
            "ts": str(int(signal.ts)),
            "signal_type": str(signal.signal_type.value if hasattr(signal.signal_type, "value") else signal.signal_type),
            "status": str(signal.status.value if hasattr(signal.status, "value") else signal.status),
            "idempotency_key": str(signal.idempotency_key),
            "payload_json": json.dumps(signal.payload or {}, separators=(",", ":"), sort_keys=True, default=str),
        }

        message_id = await self._redis.xadd(
            self._signal_stream_name,
            payload,
            maxlen=self._stream_maxlen,
            approximate=True,
        )

        self._logger.debug(
            "Published generated trade signal. stream_key=%s ts=%s signal_type=%s message_id=%s",
            signal.stream_key,
            signal.ts,
            signal.signal_type,
            message_id,
        )
        return str(message_id)