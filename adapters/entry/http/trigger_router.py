from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, field_validator


router = APIRouter(prefix="/triggers", tags=["triggers"])


class TradeCandleClosedTriggerDTO(BaseModel):
    """
    DTO for the trade stream-based candle closed trigger.
    """

    stream_key: str = Field(..., description="Canonical market-data stream key.")
    ts: int = Field(..., description="Closed candle timestamp in ms.")
    source: str = Field(..., description="Source label, e.g. binance.")
    symbol: str = Field(..., description="Trading symbol, e.g. BTCUSDT.")
    interval: str = Field(..., description='Candle interval, currently "1m".')
    candle: Dict[str, Any] | None = None

    @field_validator("stream_key", "source", "symbol", "interval")
    @classmethod
    def _strip_required_strings(cls, v: str) -> str:
        """
        Validate and normalize required string fields.
        """
        v = (v or "").strip()
        if not v:
            raise ValueError("field is required")
        return v

    @field_validator("ts")
    @classmethod
    def _validate_ts(cls, v: int) -> int:
        """
        Validate timestamp as a positive integer in milliseconds.
        """
        v = int(v)
        if v <= 0:
            raise ValueError("ts must be a positive integer (ms)")
        return v


@router.post("/trade-candle-closed")
async def trade_candle_closed_trigger(
    dto: TradeCandleClosedTriggerDTO,
    request: Request,
) -> Dict[str, Any]:
    """
    Receive and enqueue the legacy trade stream-based candle closed trigger.

    This route remains available as a manual fallback, but the normal hot path
    now comes from Redis Streams.
    """
    logger = logging.getLogger("TradeCandleClosedTrigger")

    processor = getattr(request.app.state, "trade_candle_processor", None)
    if processor is None:
        raise HTTPException(status_code=503, detail="Trade candle processor is not available.")

    asyncio.create_task(
        processor.execute(
            stream_key=dto.stream_key,
            ts=dto.ts,
            source=dto.source,
            symbol=dto.symbol,
            interval=dto.interval,
            candle=dto.candle,
        )
    )

    logger.info("Queued legacy trade_candle_closed. stream_key=%s ts=%s", dto.stream_key, dto.ts)

    return {"ok": True, "queued": True, "stream_key": dto.stream_key, "ts": dto.ts}