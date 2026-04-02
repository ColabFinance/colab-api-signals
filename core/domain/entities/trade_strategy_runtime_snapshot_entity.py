from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict

from core.domain.entities.base_entity import MongoEntity
from core.domain.enums.trade_enums import (
    TradeAtrThresholdSource,
    TradeEvent,
    TradePositionSide,
    TradeRuntimeState,
    TradeStrategyType,
)


class TradeStrategyRuntimeSnapshotEntity(MongoEntity):
    """
    Runtime snapshot for a trade strategy at a specific closed candle.

    This document stores the computed operational state of the strategy after
    evaluating the latest candle processing step.

    It is used for:
    - observability
    - dashboards
    - debugging
    - carrying strategy state across candle evaluations

    One document represents one strategy evaluated at one candle close.
    """

    strategy_id: str
    stream_key: str
    symbol: str
    interval: str
    strategy_type: TradeStrategyType

    ts: int
    open_time: int
    close_time: int

    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int = 0
    is_closed: bool = True

    atr: float = 0.0
    atr_pct: float = 0.0
    atr_value_for_threshold: float = 0.0

    atr_threshold_source: TradeAtrThresholdSource = TradeAtrThresholdSource.FIXED
    atr_low_threshold_active: Optional[float] = None
    atr_high_threshold_active: Optional[float] = None

    low_atr_hit: int = 0
    high_atr_hit: int = 0

    setup_armed: int = 0
    setup_reference_price: Optional[float] = None
    setup_reference_atr: Optional[float] = None
    setup_reference_atr_value_for_threshold: Optional[float] = None
    setup_age_bars: int = 0

    desired_side: Optional[TradePositionSide] = None
    position_side: Optional[TradePositionSide] = None

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

    event: Optional[TradeEvent] = None

    signal_up: int = 0
    signal_down: int = 0
    signal_up_first: int = 0
    signal_down_first: int = 0
    exit_signal: int = 0

    runtime_state: TradeRuntimeState = TradeRuntimeState.FLAT
    bars_since_last_event: int = 1000000

    model_config = ConfigDict(
        extra="ignore"
    )