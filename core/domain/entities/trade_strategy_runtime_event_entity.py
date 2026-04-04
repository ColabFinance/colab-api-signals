from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field

from core.domain.entities.base_entity import MongoEntity
from core.domain.enums.trade_enums import (
    TradeEvent,
    TradePositionSide,
    TradeRuntimeState,
    TradeSignalType,
)


class TradeStrategyRuntimeEventEntity(MongoEntity):
    """
    Historical runtime event for a trade strategy.

    This collection stores only relevant events instead of one document per candle.
    It preserves auditability while keeping write volume low.
    """

    idempotency_key: str

    strategy_id: str
    stream_key: str
    symbol: str
    interval: str
    ts: int

    event: Optional[TradeEvent] = None
    signal_type: Optional[TradeSignalType] = None

    runtime_state: TradeRuntimeState
    previous_runtime_state: Optional[TradeRuntimeState] = None

    position_side: Optional[TradePositionSide] = None
    previous_position_side: Optional[TradePositionSide] = None

    desired_side: Optional[TradePositionSide] = None

    setup_armed: int = 0
    previous_setup_armed: int = 0

    bars_since_last_event: int = 0

    close: float = 0.0
    atr: float = 0.0
    atr_pct: float = 0.0

    payload: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(
        extra="ignore",
    )