from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from adapters.external.redis.trade_pipeline_stream_publisher import TradePipelineStreamPublisher


class TradeCandleIngressDispatcherWorker:
    """
    Pipeline 1 worker that consumes closed candles and dispatches them to evaluation shards.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        publisher: TradePipelineStreamPublisher,
        source_stream_name: str,
        group_name: str,
        consumer_name: str,
        block_ms: int = 5000,
        read_count: int = 50,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the ingress dispatcher worker.
        """
        self._redis = redis_client
        self._publisher = publisher
        self._source_stream_name = str(source_stream_name).strip()
        self._group_name = str(group_name).strip()
        self._consumer_name = str(consumer_name).strip()
        self._block_ms = int(block_ms)
        self._read_count = int(read_count)
        self._logger = logger or logging.getLogger(self.__class__.__name__)

        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Ensure the consumer group exists and start the background loop.
        """
        if self._task is not None and not self._task.done():
            return

        await self._ensure_group()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        self._logger.info(
            "Trade candle ingress dispatcher started. stream=%s group=%s consumer=%s",
            self._source_stream_name,
            self._group_name,
            self._consumer_name,
        )

    async def stop(self) -> None:
        """
        Stop the background loop.
        """
        self._stop_event.set()

        if self._task is None:
            return

        try:
            await asyncio.wait_for(self._task, timeout=10)
        except asyncio.TimeoutError:
            self._logger.warning("Timeout waiting trade candle ingress dispatcher to stop; cancelling task.")
            self._task.cancel()
        finally:
            self._task = None

    async def _ensure_group(self) -> None:
        """
        Ensure the Redis Stream consumer group exists.
        """
        try:
            await self._redis.xgroup_create(
                name=self._source_stream_name,
                groupname=self._group_name,
                id="0",
                mkstream=True,
            )
            self._logger.info(
                "Redis consumer group created. stream=%s group=%s",
                self._source_stream_name,
                self._group_name,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            self._logger.info(
                "Redis consumer group already exists. stream=%s group=%s",
                self._source_stream_name,
                self._group_name,
            )

    async def _run_loop(self) -> None:
        """
        Run the dispatcher loop.
        """
        while not self._stop_event.is_set():
            try:
                drained_pending = await self._consume_once(stream_cursor="0", block_ms=1)
                if drained_pending:
                    continue

                await self._consume_once(stream_cursor=">", block_ms=self._block_ms)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.exception("Trade candle ingress dispatcher loop error: %s", exc)
                await asyncio.sleep(1.0)

    async def _consume_once(self, *, stream_cursor: str, block_ms: int) -> bool:
        """
        Consume and dispatch one batch from the source candle stream.
        """
        items = await self._redis.xreadgroup(
            groupname=self._group_name,
            consumername=self._consumer_name,
            streams={self._source_stream_name: stream_cursor},
            count=self._read_count,
            block=block_ms,
        )

        if not items:
            return False

        processed_any = False

        for _, messages in items:
            if not messages:
                continue

            for message_id, fields in messages:
                processed_any = True
                try:
                    payload = self._parse_event(fields)

                    shard_id, target_stream, target_message_id = await self._publisher.publish_candle_for_evaluation(
                        stream_key=payload["stream_key"],
                        ts=payload["ts"],
                        source=payload["source"],
                        symbol=payload["symbol"],
                        interval=payload["interval"],
                        candle=payload["candle"],
                    )

                    await self._redis.xack(self._source_stream_name, self._group_name, message_id)

                    self._logger.info(
                        "Dispatched candle to evaluation shard. source_message_id=%s target_message_id=%s shard=%s target_stream=%s stream_key=%s ts=%s",
                        message_id,
                        target_message_id,
                        shard_id,
                        target_stream,
                        payload["stream_key"],
                        payload["ts"],
                    )
                except Exception as exc:
                    self._logger.exception(
                        "Failed dispatching trade candle event. stream=%s message_id=%s err=%s",
                        self._source_stream_name,
                        message_id,
                        exc,
                    )
                    await asyncio.sleep(1.0)

        return processed_any

    def _parse_event(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a source candle event from Redis Streams.
        """
        stream_key = str(fields["stream_key"]).strip().lower()
        ts = int(fields["ts"])
        source = str(fields["source"]).strip().lower()
        symbol = str(fields["symbol"]).strip().upper()
        interval = str(fields["interval"]).strip().lower()

        candle = {
            "stream_key": stream_key,
            "source": source,
            "symbol": symbol,
            "interval": interval,
            "open_time": int(fields["open_time"]),
            "close_time": int(fields["close_time"]),
            "open": float(fields["open"]),
            "high": float(fields["high"]),
            "low": float(fields["low"]),
            "close": float(fields["close"]),
            "volume": float(fields.get("volume", 0.0)),
            "trades": int(fields.get("trades", 0)),
            "is_closed": self._to_bool(fields.get("is_closed", "1")),
        }

        return {
            "stream_key": stream_key,
            "ts": ts,
            "source": source,
            "symbol": symbol,
            "interval": interval,
            "candle": candle,
        }

    def _to_bool(self, value: Any) -> bool:
        """
        Convert a string-like Redis value into a boolean.
        """
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}