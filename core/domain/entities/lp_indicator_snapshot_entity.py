from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict

from core.domain.entities.base_entity import MongoEntity


class LpIndicatorSnapshotEntity(MongoEntity):
    """
    Typed LP indicator snapshot computed locally from candle history.
    """

    stream_key: str
    source: str
    symbol: str
    interval: str

    ts: int
    open_time: int
    close_time: int

    open: float
    high: float
    low: float
    close: float

    atr: float
    atr_pct: float

    entry_trend_ma: Optional[float] = None
    entry_trend_ma_prev: Optional[float] = None
    entry_trend_ma_distance_pct: Optional[float] = None
    entry_trend_ma_slope_pct: Optional[float] = None

    created_at_iso: Optional[str] = None

    model_config = ConfigDict(extra="ignore")