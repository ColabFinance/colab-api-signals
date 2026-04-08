from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from .base_entity import MongoEntity


OpenSide = Literal["down", "up"]


class AtrWidthRule(BaseModel):
    """
    ATR bucket -> target total range width.
    """
    max_atr_pct: float
    width_pct: float
    name: str = ""

    model_config = ConfigDict(extra="ignore")


class StrategyParams(BaseModel):
    """
    Live LP strategy params based on the new simple-wide-range model.

    Notes:
    - `fee_bands` was intentionally not implemented here because live fees come from the DEX.
    - Some entry-filter params are kept for payload/schema compatibility, but the live evaluator
      only uses what is available in the current indicator snapshot.
    """
    strategy_version: str = "simple_wide_lp_v1"

    # base width
    fixed_range_width_pct: float = 0.20

    # directional breakout band shares
    breakout_down_below_share: float = 0.95
    breakout_down_above_share: float = 0.05
    breakout_up_below_share: float = 0.05
    breakout_up_above_share: float = 0.95
    breakout_confirm_bars: int = 3
    breakout_use_high_low: bool = False

    # first open direction
    initial_side: OpenSide = "down"

    # ATR regime
    atr_enabled: bool = True
    atr_period: int = 14

    atr_rebalance_enabled: bool = True
    atr_rebalance_min_width_delta_pct: float = 1e-12

    atr_width_rules: List[AtrWidthRule] = Field(
        default_factory=lambda: [
            AtrWidthRule(max_atr_pct=0.0005, width_pct=0.05, name="atr_very_low_5"),
            AtrWidthRule(max_atr_pct=0.0010, width_pct=0.10, name="atr_low_10"),
            AtrWidthRule(max_atr_pct=0.0015, width_pct=0.12, name="atr_mid_12"),
            AtrWidthRule(max_atr_pct=0.0030, width_pct=0.15, name="atr_mid_high_15"),
            AtrWidthRule(max_atr_pct=float(9999999999.0), width_pct=0.20, name="atr_high_20"),
        ]
    )

    atr_hysteresis_enabled: bool = True
    atr_hysteresis_gap_pct: float = 0.0002

    atr_rebalance_cooldown_bars: int = 60
    atr_rebalance_min_age_bars: int = 30

    # execution-cost param kept for schema compatibility with backtest payloads
    swap_fee_percent: float = 0.01

    # entry filters
    entry_filters_enabled: bool = True
    allow_cash_when_filter_fails: bool = False
    entry_cooldown_bars: int = 0

    entry_atr_quantile_window: int = 200
    entry_atr_quantile: float = 0.65

    entry_trend_ma_window: int = 100
    entry_max_ma_distance_pct: float = 0.02
    entry_max_ma_slope_pct: float = 0.0015

    entry_channel_window: int = 100
    entry_channel_pos_min: float = 0.20
    entry_channel_pos_max: float = 0.80

    # compatibility with old evaluator/executor fields still stored in episode docs
    eps: float = 1e-6
    gauge_flow_enabled: bool = False

    model_config = ConfigDict(extra="allow")


class StrategyEntity(MongoEntity):
    name: str
    symbol: str
    status: str
    indicator_set_id: str
    stream_key: Optional[str] = None

    params: StrategyParams = Field(default_factory=StrategyParams)

    alias: Optional[str] = None
    dex: Optional[str] = None
    is_public: bool = True

    chain: Optional[str] = None
    owner: Optional[str] = None
    strategy_id: Optional[int] = None
    adapter: Optional[str] = None
    dex_router: Optional[str] = None
    token0: Optional[str] = None
    token1: Optional[str] = None
    tx_hash: Optional[str] = None

    model_config = ConfigDict(extra="ignore")