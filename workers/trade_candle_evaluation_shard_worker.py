from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from adapters.external.redis.trade_pipeline_stream_publisher import TradePipelineStreamPublisher
from core.usecases.process_trade_candle_closed_event_use_case import (
    ProcessTradeCandleClosedEventUseCase,
)


class TradeCandleEvaluationShardWorker:
    """
    Pipeline 2 worker that processes one evaluation shard sequentially.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        processor: ProcessTradeCandleClosedEventUseCase,
        publisher: TradePipelineStreamPublisher,
        shard_id: int,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        block_ms: int = 5000,
        read_count: int = 1,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the evaluation shard worker.
        """
        self._redis = redis_client
        self._processor = processor
        self._publisher = publisher
        self._shard_id = int(shard_id)
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
        Ensure the consumer group exists and start the background loop.
        """
        if self._task is not None and not self._task.done():
            return

        await self._ensure_group()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        self._logger.info(
            "Trade candle evaluation shard worker started. shard=%s stream=%s group=%s consumer=%s",
            self._shard_id,
            self._stream_name,
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
            self._logger.warning("Timeout waiting trade candle evaluation shard worker to stop; cancelling task.")
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
        Run the evaluation shard loop.
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
                self._logger.exception(
                    "Trade candle evaluation shard loop error. shard=%s err=%s",
                    self._shard_id,
                    exc,
                )
                await asyncio.sleep(1.0)

    async def _consume_once(self, *, stream_cursor: str, block_ms: int) -> bool:
        """
        Consume and process one batch from the evaluation shard stream.
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
                        "Evaluating candle on shard. shard=%s message_id=%s stream_key=%s ts=%s cursor=%s",
                        self._shard_id,
                        message_id,
                        payload["stream_key"],
                        payload["ts"],
                        stream_cursor,
                    )

                    signals = await self._processor.execute(
                        stream_key=payload["stream_key"],
                        ts=payload["ts"],
                        source=payload["source"],
                        symbol=payload["symbol"],
                        interval=payload["interval"],
                        candle=payload["candle"],
                    )

                    for signal in signals:
                        await self._publisher.publish_generated_signal(signal=signal)

                    await self._redis.xack(self._stream_name, self._group_name, message_id)

                    self._logger.info(
                        "Evaluation shard message acked. shard=%s message_id=%s stream_key=%s ts=%s signals=%s",
                        self._shard_id,
                        message_id,
                        payload["stream_key"],
                        payload["ts"],
                        len(signals),
                    )
                except Exception as exc:
                    self._logger.exception(
                        "Failed processing evaluation shard event. shard=%s stream=%s message_id=%s err=%s",
                        self._shard_id,
                        self._stream_name,
                        message_id,
                        exc,
                    )
                    await asyncio.sleep(1.0)

        return processed_any

    def _parse_event(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse an evaluation-shard event from Redis Streams.
        """
        stream_key = str(fields["stream_key"]).strip().lower()
        ts = int(fields["ts"])
        source = str(fields["source"]).strip().lower()
        symbol = str(fields["symbol"]).strip().upper()
        interval = str(fields["interval"]).strip().lower()
        candle = json.loads(str(fields["candle_json"]))

        return {
            "stream_key": stream_key,
            "ts": ts,
            "source": source,
            "symbol": symbol,
            "interval": interval,
            "candle": candle,
        }