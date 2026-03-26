from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class TradeStrategyRuntimeSnapshotOutDTO(BaseModel):
    """
    DTO returned when reading trade strategy runtime snapshots.
    """

    id: Optional[str] = None

    strategy_id: str
    stream_key: str
    symbol: str
    interval: str
    strategy_type: str

    ts: int
    open_time: int
    close_time: int

    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    is_closed: bool

    atr: float
    atr_pct: float
    atr_value_for_threshold: float

    low_atr_hit: int
    high_atr_hit: int

    setup_armed: int
    setup_reference_price: Optional[float] = None
    desired_side: Optional[str] = None
    position_side: Optional[str] = None

    event: Optional[str] = None

    signal_up: int
    signal_down: int
    signal_up_first: int
    signal_down_first: int
    exit_signal: int

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None