from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.domain.enums.trade_enums import (
    TradeAtrThresholdMode,
    TradeExecutionTarget,
    TradeMode,
    TradeSignalStatus,
    TradeSignalType,
    TradeStrategyStatus,
    TradeStrategyType,
)


class TradeStrategyParamsDTO(BaseModel):
    """
    Parameters for trade strategy evaluation.
    """

    atr_window: int = Field(..., ge=1)
    atr_low_threshold: float = Field(..., gt=0)
    atr_high_threshold: float = Field(..., gt=0)
    atr_threshold_mode: TradeAtrThresholdMode = Field(default=TradeAtrThresholdMode.ATR_PCT)
    cooloff_bars: int = Field(default=1, ge=0)
    trade_mode: TradeMode = Field(default=TradeMode.FLIP)
    reverse_signal: bool = Field(default=False)
    allowed_weekdays: Optional[List[str]] = Field(default=None)
    max_loss_pct: Optional[float] = Field(default=None, gt=0, le=1)

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("atr_high_threshold")
    @classmethod
    def _validate_threshold_order(cls, v: float, info) -> float:
        """
        Validate that high threshold is greater than low threshold when low is available.
        """
        low = info.data.get("atr_low_threshold")
        if low is not None and float(v) <= float(low):
            raise ValueError("atr_high_threshold must be greater than atr_low_threshold")
        return float(v)


class TradeStrategyCreateDTO(BaseModel):
    """
    DTO used to create a trade strategy.

    Trade strategies are evaluated by stream_key and do not depend on LP indicator sets.
    """

    name: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1)
    source: str = Field(default="binance", min_length=1)
    interval: str = Field(default="1m")
    stream_key: str = Field(..., min_length=1)

    strategy_type: TradeStrategyType = Field(default=TradeStrategyType.ATR_TWO_STAGE)
    status: TradeStrategyStatus = Field(default=TradeStrategyStatus.ACTIVE)

    execution_target: TradeExecutionTarget = Field(default=TradeExecutionTarget.API_TRADE_EXECUTION)
    execution_account_id: Optional[str] = None

    params: TradeStrategyParamsDTO

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("name", "symbol", "source", "interval", "stream_key", "execution_account_id")
    @classmethod
    def _strip_optional_strings(cls, v: Optional[str]) -> Optional[str]:
        """
        Normalize optional string values.
        """
        if v is None:
            return None
        return str(v).strip()

    @field_validator("symbol")
    @classmethod
    def _upper_symbol(cls, v: str) -> str:
        """
        Normalize symbol to uppercase.
        """
        return str(v).strip().upper()

    @field_validator("source", "interval", "stream_key")
    @classmethod
    def _lower_like_fields(cls, v: str) -> str:
        """
        Normalize lower-like string fields.
        """
        return str(v).strip().lower()


class TradeStrategyStatusSetDTO(BaseModel):
    """
    DTO used to update a trade strategy status.
    """

    strategy_id: str = Field(..., min_length=1)
    status: TradeStrategyStatus

    model_config = ConfigDict(use_enum_values=True)


class TradeStrategyOutDTO(BaseModel):
    """
    DTO returned by trade strategy endpoints.
    """

    id: Optional[str] = None
    name: str
    symbol: str
    source: str
    interval: str
    stream_key: str

    strategy_type: TradeStrategyType
    status: TradeStrategyStatus

    execution_target: TradeExecutionTarget
    execution_account_id: Optional[str] = None

    params: TradeStrategyParamsDTO

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None

    model_config = ConfigDict(use_enum_values=True)


class TradeSignalOutDTO(BaseModel):
    """
    DTO returned when listing generated trade signals.
    """

    id: Optional[str] = None
    strategy_id: str
    stream_key: str
    symbol: str
    interval: str
    ts: int
    signal_type: TradeSignalType
    status: TradeSignalStatus
    idempotency_key: str
    payload: dict

    attempts: int = 0
    last_error: Optional[str] = None
    execution_response: Optional[dict] = None

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None

    model_config = ConfigDict(use_enum_values=True)