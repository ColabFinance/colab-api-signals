from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from core.usecases.process_trade_candle_closed_event_use_case import (
    ProcessTradeCandleClosedEventUseCase,
)


class TradeCandleStreamConsumerWorker:
    """
    Consume closed trade candle events from Redis Streams.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        processor: ProcessTradeCandleClosedEventUseCase,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        block_ms: int = 5000,
        read_count: int = 1,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the Redis Stream consumer worker.
        """
        self._redis = redis_client
        self._processor = processor
        self._stream_name = str(stream_name).strip()
        self._group_name = str(group_name).strip()
        self._consumer_name = str(consumer_name).strip()
        self._block_ms = int(block_ms)
        self._read_count = int(read_count)
        self._logger = logger or logging.getLogger(self.__class__.__name__)

        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Create the consumer group if needed and start the background loop.
        """
        if self._task is not None and not self._task.done():
            return

        await self._ensure_group()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        self._logger.info(
            "Trade candle consumer started. stream=%s group=%s consumer=%s",
            self._stream_name,
            self._group_name,
            self._consumer_name,
        )

    async def stop(self) -> None:
        """
        Stop the background consumer loop.
        """
        self._stop_event.set()

        if self._task is None:
            return

        try:
            await asyncio.wait_for(self._task, timeout=10)
        except asyncio.TimeoutError:
            self._logger.warning("Timeout waiting trade candle consumer to stop; cancelling task.")
            self._task.cancel()
        finally:
            self._task = None

    async def _ensure_group(self) -> None:
        """
        Ensure the Redis Stream consumer group exists.
        """
        try:
            await self._redis.xgroup_create(
                name=self._stream_name,
                groupname=self._group_name,
                id="0",
                mkstream=True,
            )
            self._logger.info(
                "Redis consumer group created. stream=%s group=%s",
                self._stream_name,
                self._group_name,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
            self._logger.info(
                "Redis consumer group already exists. stream=%s group=%s",
                self._stream_name,
                self._group_name,
            )

    async def _run_loop(self) -> None:
        """
        Run the consumer loop, first draining pending messages and then reading new ones.
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
                self._logger.exception("Trade candle consumer loop error: %s", exc)
                await asyncio.sleep(1.0)

    async def _consume_once(self, *, stream_cursor: str, block_ms: int) -> bool:
        """
        Consume a single batch from the Redis Stream.

        Returns:
            True when at least one real message was processed.
            False when Redis returned no messages for this read.
        """
        items = await self._redis.xreadgroup(
            groupname=self._group_name,
            consumername=self._consumer_name,
            streams={self._stream_name: stream_cursor},
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

                    self._logger.info(
                        "Processing trade candle event. message_id=%s stream_key=%s ts=%s cursor=%s",
                        message_id,
                        payload["stream_key"],
                        payload["ts"],
                        stream_cursor,
                    )

                    await self._processor.execute(
                        stream_key=payload["stream_key"],
                        ts=payload["ts"],
                        source=payload["source"],
                        symbol=payload["symbol"],
                        interval=payload["interval"],
                        candle=payload["candle"],
                    )

                    await self._redis.xack(self._stream_name, self._group_name, message_id)

                    self._logger.info(
                        "Trade candle event acked. message_id=%s stream_key=%s ts=%s",
                        message_id,
                        payload["stream_key"],
                        payload["ts"],
                    )
                except Exception as exc:
                    self._logger.exception(
                        "Failed processing Redis trade candle event. stream=%s message_id=%s err=%s",
                        self._stream_name,
                        message_id,
                        exc,
                    )
                    await asyncio.sleep(1.0)

        return processed_any

    def _parse_event(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a Redis Stream event payload into normalized metadata plus candle data.
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