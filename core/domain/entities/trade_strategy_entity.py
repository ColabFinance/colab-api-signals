from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.domain.entities.base_entity import MongoEntity
from core.domain.enums.trade_enums import (
    TradeAtrThresholdMode,
    TradeExecutionTarget,
    TradeMode,
    TradeMovingAverageType,
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


class TradeStrategyParamsEntity(BaseModel):
    """
    Strongly typed parameter object for trade strategies.
    """

    atr_window: int = Field(..., ge=1)

    atr_low_threshold: Optional[float] = Field(default=None, gt=0)
    atr_high_threshold: Optional[float] = Field(default=None, gt=0)
    atr_threshold_mode: TradeAtrThresholdMode = TradeAtrThresholdMode.ATR_PCT

    atr_dynamic_window: Optional[int] = Field(default=None, gt=1)
    atr_dynamic_low_percentile: Optional[float] = Field(default=None)
    atr_dynamic_high_percentile: Optional[float] = Field(default=None)
    atr_dynamic_min_periods: Optional[int] = Field(default=None, ge=1)

    regime_trend_ma_window: Optional[int] = Field(default=None, gt=0)
    regime_trend_ma_type: TradeMovingAverageType = TradeMovingAverageType.EMA

    regime_structure_ma_window: Optional[int] = Field(default=None, gt=0)
    regime_structure_ma_type: TradeMovingAverageType = TradeMovingAverageType.EMA

    regime_reverse: bool = False

    min_ref_move_atr_mult: Optional[float] = Field(default=None, gt=0)
    max_setup_bars: Optional[int] = Field(default=None, gt=0)
    entry_confirm_bars: Optional[int] = Field(default=None, ge=1)
    entry_break_recent_high_window: Optional[int] = Field(default=None, gt=0)
    entry_break_recent_low_window: Optional[int] = Field(default=None, gt=0)
    min_atr_expansion_ratio: Optional[float] = Field(default=None, gt=0)

    cooloff_bars: int = Field(default=1, ge=0)
    trade_mode: TradeMode = TradeMode.FLIP
    reverse_signal: bool = False
    allowed_weekdays: Optional[List[int | str]] = None
    max_loss_pct: Optional[float] = Field(default=None, gt=0, le=1)

    stop_loss_atr_mult: Optional[float] = Field(default=None, gt=0)
    take_profit_atr_mult: Optional[float] = Field(default=None, gt=0)
    trailing_stop_atr_mult: Optional[float] = Field(default=None, gt=0)
    trailing_activation_atr_mult: Optional[float] = Field(default=None, gt=0)
    max_bars_in_trade: Optional[int] = Field(default=None, ge=1)
    exit_on_regime_flip: bool = False

    model_config = ConfigDict(
        extra="ignore"
    )

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
        "exit_on_regime_flip",
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

        Rules:
        - dynamic mode requires the 3 core dynamic fields together
        - fixed mode requires both fixed thresholds
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
            if int(self.atr_dynamic_window or 0) <= 1:
                raise ValueError("atr_dynamic_window must be greater than 1")

            low_q = float(self.atr_dynamic_low_percentile)
            high_q = float(self.atr_dynamic_high_percentile)
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