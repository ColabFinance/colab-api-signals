from __future__ import annotations
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EpisodesByVaultQuery(BaseModel):
    dex: str
    alias: str
    status: Optional[str] = None
    limit: int = Field(50, ge=1, le=500)
    offset: int = Field(0, ge=0)

    @field_validator("dex")
    @classmethod
    def validate_dex(cls, v: str) -> str:
        value = (v or "").strip().lower()
        if not value:
            raise ValueError("dex is required")
        return value

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, v: str) -> str:
        value = (v or "").strip()
        if not value:
            raise ValueError("alias is required")
        return value

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        value = (v or "").strip().upper()
        if not value:
            return None
        if value not in ("OPEN", "CLOSED"):
            raise ValueError("status must be OPEN or CLOSED")
        return value


class EpisodeRuntimeByEpisodeQuery(BaseModel):
    episode_id: str
    limit: int = Field(200, ge=1, le=2000)
    offset: int = Field(0, ge=0)

    @field_validator("episode_id")
    @classmethod
    def validate_episode_id(cls, v: str) -> str:
        value = (v or "").strip()
        if not value:
            raise ValueError("episode_id is required")
        return value


class EpisodeRuntimeByStrategyQuery(BaseModel):
    strategy_id: str
    limit: int = Field(200, ge=1, le=2000)
    offset: int = Field(0, ge=0)

    @field_validator("strategy_id")
    @classmethod
    def validate_strategy_id(cls, v: str) -> str:
        value = (v or "").strip()
        if not value:
            raise ValueError("strategy_id is required")
        return value


class StrategyEpisodeOut(BaseModel):
    id: Optional[str] = None

    strategy_id: str
    strategy_name: Optional[str] = None
    strategy_onchain_id: Optional[int] = None

    stream_key: Optional[str] = None
    symbol: str

    pool_type: str = "simple_wide"
    mode_on_open: str
    majority_on_open: str
    target_major_pct: float
    target_minor_pct: float

    open_side: Optional[str] = None
    range_width_pct: Optional[float] = None
    range_width_regime: Optional[str] = None
    atr_pct_at_open: Optional[float] = None
    entry_regime_ok: Optional[bool] = None
    entry_context: Optional[str] = None
    atr_rebalances: int = 0
    last_atr_rebalance_bar: Optional[int] = None

    open_time: int
    open_time_iso: Optional[str] = None
    open_price_signal: Optional[float] = None
    open_price_exec: Optional[float] = None

    Pa: float
    Pb: float

    band_total_width_pct: Optional[float] = None
    band_params: Dict[str, Any] = {}

    last_event_bar: int = 0
    status: str = "OPEN"

    close_time: Optional[int] = None
    close_time_iso: Optional[str] = None
    close_reason: Optional[str] = None
    close_price_exec: Optional[float] = None
    close_price_signal: Optional[float] = None

    dex: Optional[str] = None
    alias: Optional[str] = None

    metrics: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="ignore")


class StrategyEpisodeRuntimeOut(BaseModel):
    id: Optional[str] = None

    episode_id: str

    strategy_id: str
    strategy_name: Optional[str] = None
    strategy_onchain_id: Optional[int] = None

    stream_key: Optional[str] = None
    source: Optional[str] = None
    symbol: str
    interval: Optional[str] = None

    ts: int
    open_time: Optional[int] = None
    close_time: Optional[int] = None

    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None

    atr: Optional[float] = None
    atr_pct: Optional[float] = None

    entry_trend_ma: Optional[float] = None
    entry_trend_ma_prev: Optional[float] = None
    entry_trend_ma_distance_pct: Optional[float] = None
    entry_trend_ma_slope_pct: Optional[float] = None

    exec_price: Optional[float] = None

    current_pa: Optional[float] = None
    current_pb: Optional[float] = None
    current_side: Optional[str] = None
    current_range_width_pct: Optional[float] = None
    current_range_width_regime: Optional[str] = None

    out_above_streak: Optional[int] = None
    out_below_streak: Optional[int] = None
    out_above_streak_total: Optional[int] = None
    out_below_streak_total: Optional[int] = None

    above_range: Optional[bool] = None
    below_range: Optional[bool] = None

    target_side: Optional[str] = None
    target_range_width_pct: Optional[float] = None
    target_range_width_regime: Optional[str] = None
    width_delta_pct: Optional[float] = None

    breakout_up_hit: Optional[bool] = None
    breakout_down_hit: Optional[bool] = None
    atr_rebalance_hit: Optional[bool] = None

    should_close: Optional[bool] = None
    trigger_reason: Optional[str] = None
    last_event_bar: Optional[int] = None

    entry_regime_ok: Optional[bool] = None
    entry_context: Optional[str] = None

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class EpisodesByVaultResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: List[StrategyEpisodeOut] = []
    total: Optional[int] = None


class EpisodeRuntimeByEpisodeResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: List[StrategyEpisodeRuntimeOut] = []
    total: Optional[int] = None


class EpisodeRuntimeByStrategyResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: List[StrategyEpisodeRuntimeOut] = []
    total: Optional[int] = None


class EpisodeRuntimeLatestResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: Optional[StrategyEpisodeRuntimeOut] = None


class VaultRefIn(BaseModel):
    dex: str
    alias: str

    @field_validator("dex")
    @classmethod
    def validate_dex(cls, v: str) -> str:
        value = (v or "").strip().lower()
        if not value:
            raise ValueError("dex is required")
        return value

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, v: str) -> str:
        value = (v or "").strip()
        if not value:
            raise ValueError("alias is required")
        return value


class EpisodesSummaryByVaultsRequest(BaseModel):
    items: List[VaultRefIn] = Field(default_factory=list, min_length=1, max_length=1000)


class StrategyEpisodeVaultSummaryOut(BaseModel):
    dex: str
    alias: str

    total_episodes: int = 0
    open_episodes: int = 0
    closed_episodes: int = 0

    has_open_episode: bool = False
    latest_status: Optional[str] = None

    fee_total_usd: float = 0.0
    fee_24h_usd: float = 0.0

    gas_total_usd: float = 0.0
    gas_24h_usd: float = 0.0

    latest_open_time: Optional[int] = None
    latest_open_time_iso: Optional[str] = None

    latest_close_time: Optional[int] = None
    latest_close_time_iso: Optional[str] = None

    latest_updated_at: Optional[int] = None
    latest_updated_at_iso: Optional[str] = None


class EpisodesSummaryByVaultsResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: List[StrategyEpisodeVaultSummaryOut] = []
    total: int = 0