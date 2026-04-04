from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel


class TradeStrategyRuntimeSnapshotOutDTO(BaseModel):
    """
    DTO returned when reading the latest trade strategy runtime state.
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

    atr_threshold_source: str
    atr_low_threshold_active: Optional[float] = None
    atr_high_threshold_active: Optional[float] = None

    low_atr_hit: int
    high_atr_hit: int

    setup_armed: int
    setup_reference_price: Optional[float] = None
    setup_reference_atr: Optional[float] = None
    setup_reference_atr_value_for_threshold: Optional[float] = None
    setup_age_bars: int = 0

    desired_side: Optional[str] = None
    position_side: Optional[str] = None

    regime_trend_ma: Optional[float] = None
    regime_structure_ma: Optional[float] = None
    regime_allows_long: int = 1
    regime_allows_short: int = 1
    regime_allows_desired: Optional[int] = None

    ref_move_ok: Optional[int] = None
    entry_confirm_ok: Optional[int] = None
    entry_breakout_ok: Optional[int] = None
    atr_expansion_ok: Optional[int] = None

    entry_reference_price: Optional[float] = None
    entry_atr: Optional[float] = None
    entry_atr_value_for_threshold: Optional[float] = None
    bars_in_trade: int = 0

    best_favorable_price: Optional[float] = None
    worst_adverse_price: Optional[float] = None
    trailing_active: int = 0

    open_trade_loss_pct: float = 0.0

    setup_expired_now: int = 0
    stop_loss_atr_hit: int = 0
    take_profit_atr_hit: int = 0
    trailing_stop_atr_hit: int = 0
    timeout_exit_hit: int = 0
    regime_flip_exit_hit: int = 0

    event: Optional[str] = None

    signal_up: int
    signal_down: int
    signal_up_first: int
    signal_down_first: int
    exit_signal: int

    runtime_state: Optional[str] = None
    bars_since_last_event: int = 0

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None


class TradeStrategyRuntimeEventOutDTO(BaseModel):
    """
    DTO returned when reading trade strategy runtime events.
    """

    id: Optional[str] = None

    idempotency_key: str

    strategy_id: str
    stream_key: str
    symbol: str
    interval: str
    ts: int

    event: Optional[str] = None
    signal_type: Optional[str] = None

    runtime_state: str
    previous_runtime_state: Optional[str] = None

    position_side: Optional[str] = None
    previous_position_side: Optional[str] = None

    desired_side: Optional[str] = None

    setup_armed: int = 0
    previous_setup_armed: int = 0

    bars_since_last_event: int = 0

    close: float = 0.0
    atr: float = 0.0
    atr_pct: float = 0.0

    payload: Dict[str, Any] = {}

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None