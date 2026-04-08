from typing import Any, Dict, List, Optional
from pydantic import ConfigDict, Field

from core.domain.entities.strategy_entity import StrategyParams
from .base_entity import MongoEntity


class StrategyEpisodeEntity(MongoEntity):
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
    open_price_exec: Optional[float] = None
    open_price_signal: Optional[float] = None

    Pa: float
    Pb: float

    band_total_width_pct: Optional[float] = None
    band_params: StrategyParams = Field(default_factory=StrategyParams)

    last_event_bar: int = 0

    atr_streak: Dict[str, int] = Field(default_factory=dict)

    out_above_streak: int = 0
    out_below_streak: int = 0
    out_above_streak_total: int = 0
    out_below_streak_total: int = 0

    dex: Optional[str] = None
    alias: Optional[str] = None
    token0_address: Optional[str] = None
    token1_address: Optional[str] = None
    gauge_flow_enabled: bool = False

    status: str = "OPEN"
    close_time: Optional[int] = None
    close_time_iso: Optional[str] = None
    close_reason: Optional[str] = None
    close_price_signal: Optional[float] = None
    close_price_exec: Optional[float] = None

    execution_log: Optional[List[Dict[str, Any]]] = None
    metrics: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="ignore")