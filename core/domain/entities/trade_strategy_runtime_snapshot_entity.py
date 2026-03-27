from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict

from core.domain.entities.base_entity import MongoEntity
from core.domain.enums.trade_enums import (
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

    low_atr_hit: int = 0
    high_atr_hit: int = 0

    setup_armed: int = 0
    setup_reference_price: Optional[float] = None
    desired_side: Optional[TradePositionSide] = None
    position_side: Optional[TradePositionSide] = None

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