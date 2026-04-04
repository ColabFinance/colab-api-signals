from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Callable, Dict, Optional, cast

from fastapi import APIRouter, Depends, HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, Field, field_validator

from adapters.external.database.trigger_event_repository_mongodb import TriggerEventRepositoryMongoDB
from core.usecases.candle_closed_trigger_use_case import CandleClosedTriggerUseCase

from .deps import get_db


router = APIRouter(prefix="/triggers", tags=["triggers"])


class CandleClosedTriggerDTO(BaseModel):
    """
    DTO for the existing LP indicator-based candle closed trigger.
    """

    indicator_set_id: str = Field(..., description="Indicator set logical id (cfg_hash).")
    ts: int = Field(..., description="Closed candle timestamp in ms.")
    indicator_set: Optional[Dict[str, Any]] = None
    indicator_snapshot: Optional[Dict[str, Any]] = None

    @field_validator("indicator_set_id")
    @classmethod
    def _strip_id(cls, v: str) -> str:
        """
        Validate and normalize indicator_set_id.
        """
        v = (v or "").strip()
        if not v:
            raise ValueError("indicator_set_id is required")
        return v

    @field_validator("ts")
    @classmethod
    def _valid_ts(cls, v: int) -> int:
        """
        Validate timestamp as a positive integer in milliseconds.
        """
        v = int(v)
        if v <= 0:
            raise ValueError("ts must be a positive integer (ms)")
        return v


class TradeCandleClosedTriggerDTO(BaseModel):
    """
    DTO for the trade stream-based candle closed trigger.
    """

    stream_key: str = Field(..., description="Canonical market-data stream key.")
    ts: int = Field(..., description="Closed candle timestamp in ms.")
    source: str = Field(..., description="Source label, e.g. binance.")
    symbol: str = Field(..., description="Trading symbol, e.g. BTCUSDT.")
    interval: str = Field(..., description='Candle interval, currently "1m".')
    candle: Optional[Dict[str, Any]] = None

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


def _get_state_str(app: Any, key: str, default: str = "") -> str:
    """
    Read a string value from app.state safely.
    """
    try:
        return str(getattr(app.state, key, default) or default)
    except Exception:
        return default


@router.post("/candle-closed")
async def candle_closed_trigger(
    dto: CandleClosedTriggerDTO,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    """
    Receive and enqueue the existing LP indicator-based candle closed trigger.
    """
    logger = logging.getLogger("CandleClosedTrigger")

    trig_repo = TriggerEventRepositoryMongoDB(db)
    is_new = await trig_repo.mark_if_new(dto.indicator_set_id, dto.ts)
    if not is_new:
        return {"ok": True, "queued": False, "processed": False, "reason": "duplicate_event"}

    market_data_base_url = _get_state_str(request.app, "market_data_base_url", "")
    pipeline_base_url = _get_state_str(request.app, "pipeline_base_url", "")

    executor = getattr(request.app.state, "signal_executor", None)
    signal_waker: Optional[Callable[[], None]] = None
    if executor is not None:
        signal_waker = cast(Callable[[], None], executor.wake)

    uc = CandleClosedTriggerUseCase(
        db=db,
        market_data_base_url=market_data_base_url,
        pipeline_base_url=pipeline_base_url,
        max_concurrency=int(os.getenv("TRIGGER_MAX_CONCURRENCY", "10")),
        logger=logger,
        signal_waker=signal_waker,
    )

    asyncio.create_task(
        uc.execute(
            indicator_set_id=dto.indicator_set_id,
            ts=dto.ts,
            indicator_set=dto.indicator_set,
            indicator_snapshot=dto.indicator_snapshot,
        )
    )

    logger.info("Queued candle_closed. indicator_set_id=%s ts=%s", dto.indicator_set_id, dto.ts)

    return {"ok": True, "queued": True, "indicator_set_id": dto.indicator_set_id, "ts": dto.ts}


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