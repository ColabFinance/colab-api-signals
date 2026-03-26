from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class TradeStrategyParamsDTO(BaseModel):
    """
    Parameters for trade strategy evaluation.

    This DTO currently supports ATR two-stage strategies evaluated on 1m candles.
    """

    atr_window: int = Field(..., ge=1)
    atr_low_threshold: float = Field(..., gt=0)
    atr_high_threshold: float = Field(..., gt=0)
    atr_threshold_mode: Literal["atr", "atr_pct"] = Field(default="atr_pct")
    cooloff_bars: int = Field(default=1, ge=0)
    trade_mode: Literal["flip", "long_only", "short_only", "flat_on_down"] = Field(default="flip")
    reverse_signal: bool = Field(default=False)
    allowed_weekdays: Optional[List[str]] = Field(default=None)

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

    strategy_type: Literal["atr_two_stage"] = Field(default="atr_two_stage")
    status: Literal["ACTIVE", "INACTIVE"] = Field(default="ACTIVE")

    execution_target: Literal["api-trade-execution"] = Field(default="api-trade-execution")
    execution_account_id: Optional[str] = None

    params: TradeStrategyParamsDTO

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

    @field_validator("source", "interval", "stream_key", "status", "strategy_type", "execution_target")
    @classmethod
    def _lower_like_fields(cls, v: str) -> str:
        """
        Normalize lower-like fields while preserving ACTIVE/INACTIVE when needed.
        """
        raw = str(v).strip()
        if raw in {"ACTIVE", "INACTIVE"}:
            return raw
        return raw.lower()


class TradeStrategyStatusSetDTO(BaseModel):
    """
    DTO used to update a trade strategy status.
    """

    strategy_id: str = Field(..., min_length=1)
    status: Literal["ACTIVE", "INACTIVE"]


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

    strategy_type: str
    status: str

    execution_target: str
    execution_account_id: Optional[str] = None

    params: TradeStrategyParamsDTO

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None


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
    signal_type: str
    status: str
    idempotency_key: str
    payload: dict
    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None