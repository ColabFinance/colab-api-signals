from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from core.usecases.execute_trade_signal_pipeline_use_case import (
    ExecuteTradeSignalPipelineUseCase,
)


class TradeSignalStreamConsumerWorker:
    """
    Pipeline 3 worker that consumes generated trade signal events and triggers execution.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        executor_use_case: ExecuteTradeSignalPipelineUseCase,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        block_ms: int = 5000,
        read_count: int = 20,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the trade signal execution consumer worker.
        """
        self._redis = redis_client
        self._executor_use_case = executor_use_case
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
            "Trade signal execution consumer started. stream=%s group=%s consumer=%s",
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
            self._logger.warning("Timeout waiting trade signal execution consumer to stop; cancelling task.")
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
        Run the execution consumer loop.
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
                self._logger.exception("Trade signal execution consumer loop error: %s", exc)
                await asyncio.sleep(1.0)

    async def _consume_once(self, *, stream_cursor: str, block_ms: int) -> bool:
        """
        Consume and execute one batch of generated trade signal events.
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
                        "Executing generated trade signal event. message_id=%s idempotency_key=%s signal_type=%s stream_key=%s ts=%s cursor=%s",
                        message_id,
                        payload["idempotency_key"],
                        payload["signal_type"],
                        payload["stream_key"],
                        payload["ts"],
                        stream_cursor,
                    )

                    await self._executor_use_case.execute_once()
                    await self._redis.xack(self._stream_name, self._group_name, message_id)

                    self._logger.info(
                        "Trade signal event acked. message_id=%s idempotency_key=%s",
                        message_id,
                        payload["idempotency_key"],
                    )
                except Exception as exc:
                    self._logger.exception(
                        "Failed executing generated trade signal event. stream=%s message_id=%s err=%s",
                        self._stream_name,
                        message_id,
                        exc,
                    )
                    await asyncio.sleep(1.0)

        return processed_any

    def _parse_event(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a generated trade signal event from Redis Streams.
        """
        return {
            "strategy_id": str(fields["strategy_id"]),
            "stream_key": str(fields["stream_key"]).strip().lower(),
            "symbol": str(fields["symbol"]).strip().upper(),
            "interval": str(fields["interval"]).strip().lower(),
            "ts": int(fields["ts"]),
            "signal_type": str(fields["signal_type"]).strip().upper(),
            "idempotency_key": str(fields["idempotency_key"]),
            "status": str(fields["status"]).strip().upper(),
        }