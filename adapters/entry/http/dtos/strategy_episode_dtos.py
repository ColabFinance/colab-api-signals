from __future__ import annotations
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EpisodesByVaultQuery(BaseModel):
    dex: str
    alias: str
    status: Optional[str] = None
    limit: int = Field(50, ge=1, le=500)
    offset: int = Field(0, ge=0)


class StrategyEpisodeOut(BaseModel):
    id: Optional[str] = None

    strategy_id: str
    symbol: str

    pool_type: str = "standard"
    mode_on_open: str
    majority_on_open: str
    target_major_pct: float
    target_minor_pct: float

    open_time: int
    open_time_iso: Optional[str] = None
    open_price: float

    Pa: float
    Pb: float

    band_total_width_pct: Optional[float] = None
    band_params: Dict[str, Any] = {}

    last_event_bar: int = 0
    status: str = "OPEN"

    close_time: Optional[int] = None
    close_time_iso: Optional[str] = None
    close_reason: Optional[str] = None
    close_price: Optional[float] = None

    dex: Optional[str] = None
    alias: Optional[str] = None

    metrics: Optional[Dict[str, Any]] = None


class EpisodesByVaultResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: List[StrategyEpisodeOut] = []
    total: Optional[int] = None
