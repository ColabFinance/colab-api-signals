from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Dict, Literal, Optional, Tuple

from core.domain.entities.lp_indicator_snapshot_entity import LpIndicatorSnapshotEntity
from core.domain.entities.strategy_entity import StrategyEntity, StrategyParams
from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity
from core.domain.entities.strategy_episode_runtime_entity import StrategyEpisodeRuntimeEntity


EPS_POS = 1e-12
OpenSide = Literal["down", "up"]


@dataclass
class LPStrategyEvaluationResult:
    current_updates: Dict[str, Any] = field(default_factory=dict)
    should_close: bool = False
    close_reason: Optional[str] = None
    new_episode: Optional[StrategyEpisodeEntity] = None
    runtime: Optional[StrategyEpisodeRuntimeEntity] = None


class SimpleWideLPStrategyService:
    """
    Live LP evaluator for the new simple-wide-range strategy.

    This service intentionally uses only what is currently available in the
    indicator snapshot:
      - close
      - atr_pct
      - entry_trend_ma
      - entry_trend_ma_distance_pct
      - entry_trend_ma_slope_pct
      - ts

    It does not try to reproduce the full backtest-only pieces that require
    rolling candle history inside api-signals.
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _strategy_ref_id(strategy: StrategyEntity) -> str:
        if getattr(strategy, "id", None):
            return str(strategy.id).strip()
        return str(strategy.name).strip()

    @staticmethod
    def _strategy_symbol(strategy: StrategyEntity, indicator_snapshot: LpIndicatorSnapshotEntity) -> str:
        raw = str(getattr(strategy, "symbol", "") or "").strip().upper()
        if raw:
            return raw
        return str(indicator_snapshot.symbol).strip().upper()

    @staticmethod
    def _strategy_source(strategy: StrategyEntity, indicator_snapshot: LpIndicatorSnapshotEntity) -> str:
        raw = str(getattr(indicator_snapshot, "source", "") or "").strip().lower()
        if raw:
            return raw

        stream_key = str(getattr(strategy, "stream_key", "") or "").strip().lower()
        if stream_key:
            return str(stream_key.split(":")[0]).strip().lower()

        return ""

    @staticmethod
    def _ensure_valid_band(Pa: float, Pb: float, P: float) -> Tuple[float, float]:
        Pa = max(EPS_POS, float(Pa))
        Pb = max(Pa + EPS_POS, float(Pb))

        mid_pad = EPS_POS * max(1.0, float(P))
        Pa = min(float(P) - mid_pad, Pa)
        Pb = max(float(P) + mid_pad, Pb)

        if not (Pa < Pb):
            Pa = float(P) - mid_pad
            Pb = float(P) + mid_pad

        return Pa, Pb

    @staticmethod
    def _build_band(price: float, total_width_pct: float, below_share: float, above_share: float) -> Tuple[float, float]:
        width_abs = float(price) * float(total_width_pct)
        below_abs = width_abs * float(below_share)
        above_abs = width_abs * float(above_share)

        Pa = float(price) - below_abs
        Pb = float(price) + above_abs
        return SimpleWideLPStrategyService._ensure_valid_band(Pa, Pb, price)

    @staticmethod
    def _build_band_for_side(
        price: float,
        side: OpenSide,
        params: StrategyParams,
        width_pct: float,
    ) -> Tuple[float, float]:
        if side == "down":
            return SimpleWideLPStrategyService._build_band(
                price=price,
                total_width_pct=width_pct,
                below_share=params.breakout_down_below_share,
                above_share=params.breakout_down_above_share,
            )

        return SimpleWideLPStrategyService._build_band(
            price=price,
            total_width_pct=width_pct,
            below_share=params.breakout_up_below_share,
            above_share=params.breakout_up_above_share,
        )

    @staticmethod
    def _sorted_width_rules(params: StrategyParams):
        return sorted(params.atr_width_rules or [], key=lambda x: float(x.max_atr_pct))

    def _select_width_rule(self, atr_pct: float, params: StrategyParams) -> Tuple[float, str]:
        rules = self._sorted_width_rules(params)

        if not params.atr_enabled or not rules:
            return float(params.fixed_range_width_pct), "fixed"

        for rule in rules:
            if float(atr_pct) <= float(rule.max_atr_pct):
                return float(rule.width_pct), rule.name or f"width_{float(rule.width_pct):.6f}"

        last_rule = rules[-1]
        return float(last_rule.width_pct), last_rule.name or f"width_{float(last_rule.width_pct):.6f}"

    def _select_width_rule_hysteresis(
        self,
        atr_pct: float,
        current_regime_name: Optional[str],
        params: StrategyParams,
    ) -> Tuple[float, str]:
        rules = self._sorted_width_rules(params)

        if not params.atr_enabled or not rules:
            return float(params.fixed_range_width_pct), "fixed"

        if not params.atr_hysteresis_enabled or not current_regime_name:
            return self._select_width_rule(atr_pct, params)

        current_idx = None
        for idx, rule in enumerate(rules):
            if rule.name == current_regime_name:
                current_idx = idx
                break

        if current_idx is None:
            return self._select_width_rule(atr_pct, params)

        current_rule = rules[current_idx]
        gap = float(params.atr_hysteresis_gap_pct)

        if current_idx < len(rules) - 1:
            upper_trigger = float(current_rule.max_atr_pct) + gap
            if float(atr_pct) > upper_trigger:
                next_rule = rules[current_idx + 1]
                return float(next_rule.width_pct), next_rule.name or f"width_{float(next_rule.width_pct):.6f}"

        if current_idx > 0:
            prev_rule = rules[current_idx - 1]
            lower_trigger = float(prev_rule.max_atr_pct) - gap
            if float(atr_pct) <= lower_trigger:
                return float(prev_rule.width_pct), prev_rule.name or f"width_{float(prev_rule.width_pct):.6f}"

        return float(current_rule.width_pct), current_rule.name or f"width_{float(current_rule.width_pct):.6f}"

    def _select_widest_width_rule(self, params: StrategyParams) -> Tuple[float, str]:
        rules = self._sorted_width_rules(params)
        if not rules:
            return float(params.fixed_range_width_pct), "fixed_fallback"

        widest = max(rules, key=lambda x: float(x.width_pct))
        return float(widest.width_pct), f"{widest.name or 'width'}__fallback"

    def _compute_entry_regime(
        self,
        *,
        indicator_snapshot: LpIndicatorSnapshotEntity,
        P_exec: float,
        params: StrategyParams,
    ) -> Tuple[bool, str]:
        """
        Best-effort live entry filter using only current snapshot data.
        """
        if not params.entry_filters_enabled:
            return True, "filters_disabled"

        ma_distance_pct = float(indicator_snapshot.entry_trend_ma_distance_pct or 0.0)
        trend_slope_pct = float(indicator_snapshot.entry_trend_ma_slope_pct or 0.0)

        ok = (
            ma_distance_pct <= float(params.entry_max_ma_distance_pct)
            and trend_slope_pct <= float(params.entry_max_ma_slope_pct)
        )
        return bool(ok), "regime_ok" if ok else "filter_failed"

    @staticmethod
    def _majority_metadata_for_side(side: OpenSide, params: StrategyParams) -> Tuple[str, float, float]:
        if side == "down":
            return (
                "token1",
                float(params.breakout_down_below_share) * 100.0,
                float(params.breakout_down_above_share) * 100.0,
            )

        return (
            "token2",
            float(params.breakout_up_above_share) * 100.0,
            float(params.breakout_up_below_share) * 100.0,
        )

    def _build_episode(
        self,
        *,
        strategy: StrategyEntity,
        ts: int,
        symbol: str,
        P_signal: float,
        P_exec: float,
        side: OpenSide,
        width_pct: float,
        width_regime_name: str,
        atr_pct: float,
        entry_regime_ok: bool,
        entry_context: str,
        created_at_iso: Optional[str],
    ) -> StrategyEpisodeEntity:
        params = strategy.params

        Pa, Pb = self._build_band_for_side(
            price=float(P_exec),
            side=side,
            params=params,
            width_pct=float(width_pct),
        )

        majority_on_open, target_major_pct, target_minor_pct = self._majority_metadata_for_side(side, params)

        return StrategyEpisodeEntity(
            id=f"ep_{strategy.name}_{ts}",
            strategy_id=self._strategy_ref_id(strategy),
            strategy_name=str(strategy.name),
            strategy_onchain_id=(
                int(strategy.strategy_id)
                if getattr(strategy, "strategy_id", None) is not None
                else None
            ),
            stream_key=strategy.stream_key,
            symbol=symbol,
            pool_type="simple_wide",
            mode_on_open=side,
            majority_on_open=majority_on_open,
            target_major_pct=target_major_pct,
            target_minor_pct=target_minor_pct,
            open_side=side,
            range_width_pct=float(width_pct),
            range_width_regime=width_regime_name,
            atr_pct_at_open=float(atr_pct),
            entry_regime_ok=bool(entry_regime_ok),
            entry_context=entry_context,
            open_time=int(ts),
            open_time_iso=created_at_iso,
            open_price_signal=float(P_signal),
            open_price_exec=float(P_exec),
            Pa=float(Pa),
            Pb=float(Pb),
            band_total_width_pct=float(width_pct),
            band_params=params.model_copy(deep=True),
            last_event_bar=0,
            atr_streak={},
            out_above_streak=0,
            out_below_streak=0,
            out_above_streak_total=0,
            out_below_streak_total=0,
            dex=strategy.dex,
            alias=strategy.alias,
            token0_address=strategy.token0,
            token1_address=strategy.token1,
            gauge_flow_enabled=bool(params.gauge_flow_enabled),
            atr_rebalances=0,
            last_atr_rebalance_bar=None,
        )

    def _build_runtime(
        self,
        *,
        strategy: StrategyEntity,
        episode: StrategyEpisodeEntity,
        indicator_snapshot: LpIndicatorSnapshotEntity,
        P_exec: float,
        current_side: str,
        current_width_pct: float,
        current_width_regime: str,
        target_side: str,
        target_width_pct: float,
        target_width_regime: str,
        out_above_streak: int,
        out_below_streak: int,
        out_above_streak_total: int,
        out_below_streak_total: int,
        width_delta_pct: float,
        should_close: bool,
        trigger_reason: Optional[str],
        last_event_bar: int,
    ) -> StrategyEpisodeRuntimeEntity:
        params = strategy.params
        eps = float(params.eps)

        above_range = float(P_exec) > float(episode.Pb) * (1.0 + eps)
        below_range = float(P_exec) < float(episode.Pa) * (1.0 - eps)

        confirm_bars = max(1, int(params.breakout_confirm_bars))
        breakout_up_hit = out_above_streak >= confirm_bars
        breakout_down_hit = out_below_streak >= confirm_bars
        atr_rebalance_hit = bool(trigger_reason and str(trigger_reason).startswith("atr_width_rebalance:"))

        return StrategyEpisodeRuntimeEntity(
            episode_id=str(episode.id),
            strategy_id=self._strategy_ref_id(strategy),
            strategy_name=str(strategy.name),
            strategy_onchain_id=(
                int(strategy.strategy_id)
                if getattr(strategy, "strategy_id", None) is not None
                else None
            ),
            stream_key=strategy.stream_key,
            source=self._strategy_source(strategy, indicator_snapshot),
            symbol=self._strategy_symbol(strategy, indicator_snapshot),
            interval=indicator_snapshot.interval,
            ts=int(indicator_snapshot.ts),
            open_time=int(indicator_snapshot.open_time),
            close_time=int(indicator_snapshot.close_time),
            open=float(indicator_snapshot.open),
            high=float(indicator_snapshot.high),
            low=float(indicator_snapshot.low),
            close=float(indicator_snapshot.close),
            atr=float(indicator_snapshot.atr),
            atr_pct=float(indicator_snapshot.atr_pct),
            entry_trend_ma=(
                float(indicator_snapshot.entry_trend_ma)
                if indicator_snapshot.entry_trend_ma is not None
                else None
            ),
            entry_trend_ma_prev=(
                float(indicator_snapshot.entry_trend_ma_prev)
                if indicator_snapshot.entry_trend_ma_prev is not None
                else None
            ),
            entry_trend_ma_distance_pct=(
                float(indicator_snapshot.entry_trend_ma_distance_pct)
                if indicator_snapshot.entry_trend_ma_distance_pct is not None
                else None
            ),
            entry_trend_ma_slope_pct=(
                float(indicator_snapshot.entry_trend_ma_slope_pct)
                if indicator_snapshot.entry_trend_ma_slope_pct is not None
                else None
            ),
            exec_price=float(P_exec),
            current_pa=float(episode.Pa),
            current_pb=float(episode.Pb),
            current_side=str(current_side),
            current_range_width_pct=float(current_width_pct),
            current_range_width_regime=str(current_width_regime),
            out_above_streak=int(out_above_streak),
            out_below_streak=int(out_below_streak),
            out_above_streak_total=int(out_above_streak_total),
            out_below_streak_total=int(out_below_streak_total),
            above_range=bool(above_range),
            below_range=bool(below_range),
            target_side=str(target_side),
            target_range_width_pct=float(target_width_pct),
            target_range_width_regime=str(target_width_regime),
            width_delta_pct=float(width_delta_pct),
            breakout_up_hit=bool(breakout_up_hit),
            breakout_down_hit=bool(breakout_down_hit),
            atr_rebalance_hit=bool(atr_rebalance_hit),
            should_close=bool(should_close),
            trigger_reason=trigger_reason,
            last_event_bar=int(last_event_bar),
            entry_regime_ok=episode.entry_regime_ok,
            entry_context=episode.entry_context,
        )

    def build_initial_runtime(
        self,
        *,
        strategy: StrategyEntity,
        episode: StrategyEpisodeEntity,
        indicator_snapshot: LpIndicatorSnapshotEntity,
        P_exec: float,
    ) -> StrategyEpisodeRuntimeEntity:
        current_width_pct = float(
            episode.range_width_pct
            if episode.range_width_pct is not None
            else (
                episode.band_total_width_pct
                if episode.band_total_width_pct is not None
                else strategy.params.fixed_range_width_pct
            )
        )
        current_width_regime = str(episode.range_width_regime or "fixed")
        current_side = str(episode.open_side or episode.mode_on_open or strategy.params.initial_side)

        return self._build_runtime(
            strategy=strategy,
            episode=episode,
            indicator_snapshot=indicator_snapshot,
            P_exec=float(P_exec),
            current_side=current_side,
            current_width_pct=current_width_pct,
            current_width_regime=current_width_regime,
            target_side=current_side,
            target_width_pct=current_width_pct,
            target_width_regime=current_width_regime,
            out_above_streak=int(episode.out_above_streak or 0),
            out_below_streak=int(episode.out_below_streak or 0),
            out_above_streak_total=int(episode.out_above_streak_total or 0),
            out_below_streak_total=int(episode.out_below_streak_total or 0),
            width_delta_pct=0.0,
            should_close=False,
            trigger_reason=None,
            last_event_bar=int(episode.last_event_bar or 0),
        )

    def build_initial_episode(
        self,
        *,
        strategy: StrategyEntity,
        indicator_snapshot: LpIndicatorSnapshotEntity,
        symbol: str,
        P_signal: float,
        P_exec: float,
        ts: int,
    ) -> StrategyEpisodeEntity:
        params = strategy.params
        atr_pct = float(indicator_snapshot.atr_pct or 0.0)
        created_at_iso = indicator_snapshot.created_at_iso

        target_width_pct, target_width_regime = self._select_width_rule(atr_pct, params)
        entry_regime_ok, entry_context = self._compute_entry_regime(
            indicator_snapshot=indicator_snapshot,
            P_exec=float(P_exec),
            params=params,
        )

        width_to_use = float(target_width_pct)
        width_regime_to_use = target_width_regime

        if not entry_regime_ok:
            fallback_width, fallback_regime = self._select_widest_width_rule(params)
            width_to_use = float(fallback_width)
            width_regime_to_use = fallback_regime

            if params.allow_cash_when_filter_fails:
                entry_context = "cash_mode_requested_but_not_modeled_live_fallback_max_width"
            else:
                entry_context = "fallback_max_width"

        return self._build_episode(
            strategy=strategy,
            ts=ts,
            symbol=symbol,
            P_signal=float(P_signal),
            P_exec=float(P_exec),
            side=params.initial_side,
            width_pct=width_to_use,
            width_regime_name=width_regime_to_use,
            atr_pct=atr_pct,
            entry_regime_ok=entry_regime_ok,
            entry_context=entry_context,
            created_at_iso=created_at_iso,
        )

    def evaluate_current_episode(
        self,
        *,
        strategy: StrategyEntity,
        current: StrategyEpisodeEntity,
        indicator_snapshot: LpIndicatorSnapshotEntity,
        symbol: str,
        P_signal: float,
        P_exec: float,
        ts: int,
    ) -> LPStrategyEvaluationResult:
        params = strategy.params
        atr_pct = float(indicator_snapshot.atr_pct or 0.0)
        created_at_iso = indicator_snapshot.created_at_iso

        current_side = str(current.open_side or current.mode_on_open or params.initial_side).lower()
        if current_side not in ("down", "up"):
            current_side = params.initial_side

        current_width_pct = float(
            current.range_width_pct
            if current.range_width_pct is not None
            else (
                current.band_total_width_pct
                if current.band_total_width_pct is not None
                else params.fixed_range_width_pct
            )
        )

        current_regime_name = str(current.range_width_regime or "fixed")
        i_since_open = int(current.last_event_bar or 0) + 1

        target_width_pct, target_width_regime = self._select_width_rule_hysteresis(
            atr_pct=float(atr_pct),
            current_regime_name=current_regime_name,
            params=params,
        )

        Pa_cur = float(current.Pa)
        Pb_cur = float(current.Pb)
        eps = float(params.eps)

        out_above_streak = int(current.out_above_streak or 0)
        out_below_streak = int(current.out_below_streak or 0)
        out_above_streak_total = int(current.out_above_streak_total or 0)
        out_below_streak_total = int(current.out_below_streak_total or 0)

        above = float(P_exec) > float(Pb_cur) * (1.0 + eps)
        below = float(P_exec) < float(Pa_cur) * (1.0 - eps)

        if above:
            out_above_streak += 1
            out_below_streak = 0
            out_above_streak_total += 1
        elif below:
            out_above_streak = 0
            out_below_streak += 1
            out_below_streak_total += 1
        else:
            out_above_streak = 0
            out_below_streak = 0

        updates: Dict[str, Any] = {
            "last_event_bar": i_since_open,
            "out_above_streak": out_above_streak,
            "out_below_streak": out_below_streak,
            "out_above_streak_total": out_above_streak_total,
            "out_below_streak_total": out_below_streak_total,
        }

        trigger_reason: Optional[str] = None
        next_side: OpenSide = current_side  # type: ignore[assignment]

        width_delta = abs(float(current_width_pct) - float(target_width_pct))
        if (
            params.atr_enabled
            and params.atr_rebalance_enabled
            and width_delta >= float(params.atr_rebalance_min_width_delta_pct)
            and i_since_open >= int(params.atr_rebalance_min_age_bars)
        ):
            trigger_reason = f"atr_width_rebalance:{current_regime_name}->{target_width_regime}"
            next_side = current_side  # type: ignore[assignment]
        else:
            confirm_bars = max(1, int(params.breakout_confirm_bars))
            if out_above_streak >= confirm_bars:
                trigger_reason = "breakout_up_close_confirmed"
                next_side = "up"
            elif out_below_streak >= confirm_bars:
                trigger_reason = "breakout_down_close_confirmed"
                next_side = "down"

        if not trigger_reason:
            runtime = self._build_runtime(
                strategy=strategy,
                episode=current,
                indicator_snapshot=indicator_snapshot,
                P_exec=float(P_exec),
                current_side=current_side,
                current_width_pct=current_width_pct,
                current_width_regime=current_regime_name,
                target_side=current_side,
                target_width_pct=float(target_width_pct),
                target_width_regime=target_width_regime,
                out_above_streak=out_above_streak,
                out_below_streak=out_below_streak,
                out_above_streak_total=out_above_streak_total,
                out_below_streak_total=out_below_streak_total,
                width_delta_pct=float(width_delta),
                should_close=False,
                trigger_reason=None,
                last_event_bar=i_since_open,
            )
            return LPStrategyEvaluationResult(
                current_updates=updates,
                runtime=runtime,
            )

        entry_regime_ok, entry_context = self._compute_entry_regime(
            indicator_snapshot=indicator_snapshot,
            P_exec=float(P_exec),
            params=params,
        )

        width_to_open = float(target_width_pct)
        width_regime_to_open = target_width_regime

        if not entry_regime_ok:
            fallback_width, fallback_regime = self._select_widest_width_rule(params)
            width_to_open = float(fallback_width)
            width_regime_to_open = fallback_regime

            if params.allow_cash_when_filter_fails:
                entry_context = "cash_mode_requested_but_not_modeled_live_fallback_max_width"
            else:
                entry_context = "fallback_max_width"

        new_episode = self._build_episode(
            strategy=strategy,
            ts=ts,
            symbol=symbol,
            P_signal=float(P_signal),
            P_exec=float(P_exec),
            side=next_side,
            width_pct=width_to_open,
            width_regime_name=width_regime_to_open,
            atr_pct=float(atr_pct),
            entry_regime_ok=entry_regime_ok,
            entry_context=entry_context,
            created_at_iso=created_at_iso,
        )

        runtime = self._build_runtime(
            strategy=strategy,
            episode=current,
            indicator_snapshot=indicator_snapshot,
            P_exec=float(P_exec),
            current_side=current_side,
            current_width_pct=current_width_pct,
            current_width_regime=current_regime_name,
            target_side=next_side,
            target_width_pct=float(width_to_open),
            target_width_regime=width_regime_to_open,
            out_above_streak=out_above_streak,
            out_below_streak=out_below_streak,
            out_above_streak_total=out_above_streak_total,
            out_below_streak_total=out_below_streak_total,
            width_delta_pct=float(width_delta),
            should_close=True,
            trigger_reason=trigger_reason,
            last_event_bar=i_since_open,
        )

        return LPStrategyEvaluationResult(
            current_updates=updates,
            should_close=True,
            close_reason=trigger_reason,
            new_episode=new_episode,
            runtime=runtime,
        )