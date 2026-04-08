from __future__ import annotations

from typing import Optional

from pydantic import ConfigDict

from core.domain.entities.base_entity import MongoEntity


class StrategyEpisodeRuntimeEntity(MongoEntity):
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

    model_config = ConfigDict(extra="ignore")