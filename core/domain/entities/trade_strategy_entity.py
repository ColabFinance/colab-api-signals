from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from core.domain.entities.base_entity import MongoEntity
from core.domain.enums.trade_enums import (
    TradeAtrThresholdMode,
    TradeExecutionTarget,
    TradeMode,
    TradeStrategyStatus,
    TradeStrategyType,
)


class TradeStrategyParamsEntity(BaseModel):
    """
    Strongly typed parameter object for trade strategies.

    This entity supports ATR two-stage strategy configuration and keeps
    the trade flow independent from LP indicator sets.
    """

    atr_window: int
    atr_low_threshold: float
    atr_high_threshold: float
    atr_threshold_mode: TradeAtrThresholdMode = TradeAtrThresholdMode.ATR_PCT
    cooloff_bars: int = 1
    trade_mode: TradeMode = TradeMode.FLIP
    reverse_signal: bool = False
    allowed_weekdays: Optional[List[str]] = None
    max_loss_pct: Optional[float] = None

    model_config = ConfigDict(
        extra="ignore"
    )


class TradeStrategyEntity(MongoEntity):
    """
    Canonical trade strategy entity stored in MongoDB.

    One strategy belongs to one stream_key and is evaluated whenever
    a 1m candle closes for that stream.
    """

    name: str
    symbol: str
    source: str
    interval: str
    stream_key: str

    strategy_type: TradeStrategyType = TradeStrategyType.ATR_TWO_STAGE
    status: TradeStrategyStatus = TradeStrategyStatus.ACTIVE

    execution_target: TradeExecutionTarget = TradeExecutionTarget.API_TRADE_EXECUTION
    execution_account_id: Optional[str] = None

    params: TradeStrategyParamsEntity = Field(...)

    model_config = ConfigDict(
        extra="ignore"
    )