from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any, Dict, Optional

from redis.asyncio.client import Redis

from core.usecases.process_lp_candle_closed_event_use_case import ProcessLpCandleClosedEventUseCase


class LpCandleEvaluationShardWorker:
    """
    Consumes LP evaluation events from one shard stream and executes the LP processor.
    """

    def __init__(
        self,
        *,
        redis_client: Redis,
        processor: ProcessLpCandleClosedEventUseCase,
        shard_id: int,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        block_ms: int,
        read_count: int,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._redis = redis_client
        self._processor = processor
        self._shard_id = int(shard_id)
        self._stream_name = str(stream_name)
        self._group_name = str(group_name)
        self._consumer_name = str(consumer_name)
        self._block_ms = int(block_ms)
        self._read_count = int(read_count)
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await self._ensure_group()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
            self._task = None

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self._stream_name,
                groupname=self._group_name,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def _decode_fields(self, fields: Dict[Any, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in fields.items():
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            out[str(key)] = str(value)
        return out

    async def _run(self) -> None:
        while True:
            entries = await self._redis.xreadgroup(
                groupname=self._group_name,
                consumername=self._consumer_name,
                streams={self._stream_name: ">"},
                count=self._read_count,
                block=self._block_ms,
            )
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, raw_fields in messages:
                    fields = self._decode_fields(raw_fields)
                    candle = None
                    if fields.get("candle"):
                        try:
                            candle = json.loads(fields["candle"])
                        except Exception:
                            candle = None

                    try:
                        await self._processor.execute(
                            stream_key=fields["stream_key"],
                            ts=int(fields["ts"]),
                            source=fields["source"],
                            symbol=fields["symbol"],
                            interval=fields["interval"],
                            candle=candle,
                        )
                    except Exception as exc:
                        self._logger.exception(
                            "Failed processing LP candle shard=%s stream=%s ts=%s err=%s",
                            self._shard_id,
                            fields.get("stream_key"),
                            fields.get("ts"),
                            exc,
                        )
                    finally:
                        await self._redis.xack(self._stream_name, self._group_name, msg_id)