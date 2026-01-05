import asyncio
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Request
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, Field, field_validator

from adapters.external.database.trigger_event_repository_mongodb import TriggerEventRepositoryMongoDB
from core.usecases.candle_closed_trigger_use_case import CandleClosedTriggerUseCase

from .deps import get_db


router = APIRouter(prefix="/triggers", tags=["triggers"])


class CandleClosedTriggerDTO(BaseModel):
    indicator_set_id: str = Field(..., description="Indicator set logical id (cfg_hash).")
    ts: int = Field(..., description="Closed candle timestamp in ms.")
    indicator_set: Optional[Dict[str, Any]] = None
    indicator_snapshot: Optional[Dict[str, Any]] = None

    @field_validator("indicator_set_id")
    @classmethod
    def _strip_id(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("indicator_set_id is required")
        return v

    @field_validator("ts")
    @classmethod
    def _valid_ts(cls, v: int) -> int:
        v = int(v)
        if v <= 0:
            raise ValueError("ts must be a positive integer (ms)")
        return v


def _get_state_str(app: Any, key: str, default: str = "") -> str:
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
    logger = logging.getLogger("CandleClosedTrigger")

    # 1) Idempotency guard (fast)
    trig_repo = TriggerEventRepositoryMongoDB(db)
    is_new = await trig_repo.mark_if_new(dto.indicator_set_id, dto.ts)
    if not is_new:
        return {"ok": True, "queued": False, "processed": False, "reason": "duplicate_event"}

    # 2) Enqueue background processing
    market_data_base_url = _get_state_str(request.app, "market_data_base_url", "")
    pipeline_base_url = _get_state_str(request.app, "pipeline_base_url", "")

    uc = CandleClosedTriggerUseCase(
        db=db,
        market_data_base_url=market_data_base_url,
        pipeline_base_url=pipeline_base_url,
        max_concurrency=int(getattr(request.app.state, "trigger_max_concurrency", 10) or 10),
        logger=logger,
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
