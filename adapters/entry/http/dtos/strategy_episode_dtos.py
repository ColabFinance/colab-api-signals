from __future__ import annotations
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


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


class StrategyEpisodeOut(BaseModel):
    id: Optional[str] = None

    strategy_id: str
    symbol: str

    pool_type: str = "standard"
    mode_on_open: str
    majority_on_open: str
    target_major_pct: float
    target_minor_pct: float

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


class EpisodesByVaultResponse(BaseModel):
    ok: bool = True
    message: str = "ok"
    data: List[StrategyEpisodeOut] = []
    total: Optional[int] = None


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