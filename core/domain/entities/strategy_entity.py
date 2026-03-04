from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .base_entity import MongoEntity


class RangeTierParams(BaseModel):
    """
    Mirrors backtest RangeTierCfg.
    max_major_side_pct is interpreted as TOTAL WIDTH (fraction) of the range.
    """
    name: str
    atr_pct_threshold: float
    atr_pct_threshold_down: float
    bars_required: int
    max_major_side_pct: float
    allowed_from: List[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="ignore")


class StrategyParams(BaseModel):
    """
    Typed params object (no more dict.get in runtime code).

    We still allow extra fields for backward compatibility with existing DB docs.
    """
    # operation
    eps: float = 1e-6
    cooloff_bars: int = 1
    breakout_confirm_bars: int = 1
    gauge_flow_enabled: bool = False

    # skew base (non-high-vol pools/tiers)
    skew_low_pct: float = 0.09
    skew_high_pct: float = 0.01

    # HIGH VOL behavior (from backtest)
    block_high_vol_up_atr_pct: Optional[float] = None

    high_vol_base_below_pct: float = 0.099
    high_vol_base_above_pct: float = 0.001
    high_vol_invert_on_trend_up: bool = True

    # TOTAL WIDTH of high-vol pool range (fraction)
    high_vol_max_major_side_pct: float = 2.0

    # tiers
    tiers: List[RangeTierParams] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


class StrategyEntity(MongoEntity):
    name: str
    symbol: str
    status: str
    indicator_set_id: str
    stream_key: str

    params: StrategyParams = Field(default_factory=StrategyParams)

    alias: Optional[str] = None
    dex: Optional[str] = None
    chain: Optional[str] = None  # "base" | "bnb"
    owner: Optional[str] = None  # lowercase 0x...
    strategy_id: Optional[int] = None  # onchain strategyId (uint)
    adapter: Optional[str] = None
    dex_router: Optional[str] = None
    token0: Optional[str] = None
    token1: Optional[str] = None
    tx_hash: Optional[str] = None  # tx that created/updated this registry data

    model_config = ConfigDict(extra="ignore")