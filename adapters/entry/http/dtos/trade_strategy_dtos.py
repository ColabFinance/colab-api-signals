from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.domain.enums.trade_enums import (
    TradeAtrThresholdMode,
    TradeExecutionTarget,
    TradeMode,
    TradeMovingAverageType,
    TradeSignalStatus,
    TradeSignalType,
    TradeStrategyStatus,
    TradeStrategyType,
)


def _normalize_percentile_input(v: Optional[float]) -> Optional[float]:
    """
    Normalize percentile input.

    Accepted formats:
    - 0.2 / 0.6
    - 20 / 60
    """
    if v is None:
        return None

    q = float(v)
    if 1.0 < q <= 100.0:
        q = q / 100.0

    if not (0.0 <= q <= 1.0):
        raise ValueError("Percentile must be between 0 and 1, or between 0 and 100.")

    return q


class TradeStrategyParamsDTO(BaseModel):
    """
    Parameters for trade strategy evaluation.

    Supports:
    - legacy fixed ATR thresholds
    - dynamic ATR thresholds
    - regime filters based on moving averages
    """

    atr_window: int = Field(..., ge=1)

    atr_low_threshold: Optional[float] = Field(default=None, gt=0)
    atr_high_threshold: Optional[float] = Field(default=None, gt=0)
    atr_threshold_mode: TradeAtrThresholdMode = Field(default=TradeAtrThresholdMode.ATR_PCT)

    atr_dynamic_window: Optional[int] = Field(default=None, gt=1)
    atr_dynamic_low_percentile: Optional[float] = Field(default=None)
    atr_dynamic_high_percentile: Optional[float] = Field(default=None)
    atr_dynamic_min_periods: Optional[int] = Field(default=None, ge=1)

    regime_trend_ma_window: Optional[int] = Field(default=None, gt=0)
    regime_trend_ma_type: TradeMovingAverageType = Field(default=TradeMovingAverageType.EMA)

    regime_structure_ma_window: Optional[int] = Field(default=None, gt=0)
    regime_structure_ma_type: TradeMovingAverageType = Field(default=TradeMovingAverageType.EMA)

    regime_reverse: bool = Field(default=False)

    cooloff_bars: int = Field(default=1, ge=0)
    trade_mode: TradeMode = Field(default=TradeMode.FLIP)
    reverse_signal: bool = Field(default=False)
    allowed_weekdays: Optional[List[int | str]] = Field(default=None)
    max_loss_pct: Optional[float] = Field(default=None, gt=0, le=1)

    model_config = ConfigDict(use_enum_values=True)

    @field_validator(
        "atr_dynamic_low_percentile",
        "atr_dynamic_high_percentile",
        mode="before",
    )
    @classmethod
    def _normalize_dynamic_percentiles(cls, v):
        return _normalize_percentile_input(v)

    @field_validator(
        "regime_reverse",
        "reverse_signal",
        mode="before",
    )
    @classmethod
    def _normalize_optional_bool(cls, v):
        if v is None:
            return False
        return v

    @model_validator(mode="after")
    def _validate_threshold_configuration(self):
        """
        Validate fixed-vs-dynamic ATR threshold configuration.
        """
        dynamic_core = [
            self.atr_dynamic_window,
            self.atr_dynamic_low_percentile,
            self.atr_dynamic_high_percentile,
        ]
        dynamic_enabled = all(x is not None for x in dynamic_core)
        dynamic_partial = any(x is not None for x in dynamic_core) and not dynamic_enabled

        if dynamic_partial:
            raise ValueError(
                "To enable dynamic ATR thresholds, set "
                "atr_dynamic_window, atr_dynamic_low_percentile and "
                "atr_dynamic_high_percentile together."
            )

        if dynamic_enabled:
            low_q = float(self.atr_dynamic_low_percentile)  # already normalized
            high_q = float(self.atr_dynamic_high_percentile)  # already normalized
            if high_q <= low_q:
                raise ValueError(
                    "atr_dynamic_high_percentile must be greater than "
                    "atr_dynamic_low_percentile"
                )
        else:
            if self.atr_low_threshold is None:
                raise ValueError(
                    "atr_low_threshold is required when dynamic ATR thresholds are disabled"
                )
            if self.atr_high_threshold is None:
                raise ValueError(
                    "atr_high_threshold is required when dynamic ATR thresholds are disabled"
                )
            if float(self.atr_high_threshold) <= float(self.atr_low_threshold):
                raise ValueError("atr_high_threshold must be greater than atr_low_threshold")

        return self


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