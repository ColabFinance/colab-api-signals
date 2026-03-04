from datetime import datetime, time, timezone
import logging
from typing import Callable, Dict, List, Optional, Tuple

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from core.domain.entities.signal_entity import SignalEntity, SignalStep
from core.domain.entities.strategy_entity import RangeTierParams, StrategyEntity, StrategyParams
from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity
from core.domain.enums.signal_enums import SignalStatus, SignalType

from ..services.strategy_reconciler_service import StrategyReconcilerService

from ..repositories.strategy_repository import StrategyRepository
from ..repositories.strategy_episode_repository import StrategyEpisodeRepository
from ..repositories.signal_repository import SignalRepository


class EvaluateActiveStrategiesUseCase:
    """
    Evaluates all ACTIVE strategies tied to a given indicator set when a new snapshot arrives.
    Applies gates similar to the backtest (breakout, high-vol, tiers + cooldown)
    and reconciles desired episode with Liquidity Provider by emitting signals (PENDING).
    """

    def __init__(
        self,
        strategy_repo: StrategyRepository,
        episode_repo: StrategyEpisodeRepository,
        signal_repo: SignalRepository,
        reconciling_service: StrategyReconcilerService,
        lp_client: PipelineHttpClient,
        logger: Optional[logging.Logger] = None,
        on_signal_created: Optional[Callable[[], None]] = None,
    ):
        self._strategy_repo = strategy_repo
        self._episode_repo = episode_repo
        self._signal_repo = signal_repo
        self._reconciler = reconciling_service
        self._lp_client = lp_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._on_signal_created = on_signal_created
        self.market_data = MarketDataHttpClient.from_settings()
    
    # ==========================
    # Trend / Band helpers
    # ==========================
    
    @staticmethod
    def _trend_at(ema_fast_val: float, ema_slow_val: float) -> str:
        return "up" if ema_fast_val > ema_slow_val else "down"

    @staticmethod
    def _ensure_valid_band(Pa: float, Pb: float, P: float) -> Tuple[float, float]:
        EPS_POS = 1e-12
        Pa = max(EPS_POS, float(Pa))
        Pb = max(Pa + EPS_POS, float(Pb))
        mid_pad = EPS_POS * max(1.0, float(P))
        Pa = min(P - mid_pad, Pa)
        Pb = max(P + mid_pad, Pb)
        if not (Pa < Pb):
            Pa = P - mid_pad
            Pb = P + mid_pad
        return Pa, Pb

    @staticmethod
    def _scale_to_total_width(pct_below_base: float, pct_above_base: float, total_width_pct: float) -> Tuple[float, float]:
        base_sum = pct_below_base + pct_above_base
        if base_sum <= 0:
            half = max(1e-12, total_width_pct / 2.0)
            return half, half
        scale = total_width_pct / base_sum
        return pct_below_base * scale, pct_above_base * scale

    @staticmethod
    def _is_in_range(P: float, Pa: float, Pb: float, eps: float) -> bool:
        return (P > Pa * (1.0 + eps)) and (P < Pb * (1.0 - eps))

    @staticmethod
    def _parse_pool_status(st: Dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Returns (P_pool, Pa_pool, Pb_pool) when available/valid, else None(s).
        """
        prices = (st.get("prices", {}) or {})

        # --- current price ---
        P_pool: Optional[float] = None
        cur = prices.get("current", {}) or {}
        p_raw = cur.get("p_t1_t0", None)
        if p_raw is not None:
            p = float(p_raw)
            if p > 0.0:
                P_pool = p

        # --- range ---
        Pa_pool: Optional[float] = None
        Pb_pool: Optional[float] = None

        t_lower_raw = st.get("lower", None)
        t_upper_raw = st.get("upper", None)

        p_low_blk = prices.get("lower", {}) or {}
        p_up_blk = prices.get("upper", {}) or {}

        t_low_blk = p_low_blk.get("tick", None)
        t_up_blk = p_up_blk.get("tick", None)

        tA = t_lower_raw if t_lower_raw is not None else t_low_blk
        tB = t_upper_raw if t_upper_raw is not None else t_up_blk

        if tA is not None and tB is not None:
            tA = int(tA)
            tB = int(tB)
            t_low = min(tA, tB)
            t_up  = max(tA, tB)

            if t_low_blk is not None and int(t_low_blk) == t_low:
                pa_raw = p_low_blk.get("p_t1_t0", None)
                pb_raw = p_up_blk.get("p_t1_t0", None)
            elif t_up_blk is not None and int(t_up_blk) == t_low:
                pa_raw = p_up_blk.get("p_t1_t0", None)
                pb_raw = p_low_blk.get("p_t1_t0", None)
            else:
                pa_raw = p_low_blk.get("p_t1_t0", None)
                pb_raw = p_up_blk.get("p_t1_t0", None)

            if pa_raw is not None and pb_raw is not None:
                pa = float(pa_raw)
                pb = float(pb_raw)
                if pa > 0.0 and pb > 0.0:
                    Pa_pool, Pb_pool = (pa, pb) if pa < pb else (pb, pa)

        return P_pool, Pa_pool, Pb_pool

    async def _pick_band_for_trend_totalwidth(
        self,
        *,
        P: float,
        trend: str,
        params: StrategyParams,
        total_width_override: Optional[float],
        pool_type: str,
    ) -> Tuple[float, float, str, str, float, float]:
        """
        Backtest parity for band construction.
        Returns: (Pa, Pb, mode, majority, pct_below_base, pct_above_base)
        """

        if pool_type == "high_vol":
            if trend == "down":
                majority = "token1"
                mode = "trend_down"
                pct_below_base = float(params.high_vol_base_below_pct)
                pct_above_base = float(params.high_vol_base_above_pct)
            else:
                majority = "token2"
                mode = "trend_up"
                if bool(params.high_vol_invert_on_trend_up):
                    pct_below_base = float(params.high_vol_base_above_pct)
                    pct_above_base = float(params.high_vol_base_below_pct)
                else:
                    pct_below_base = float(params.high_vol_base_below_pct)
                    pct_above_base = float(params.high_vol_base_above_pct)
        else:
            # tiers (and any non-high_vol) uses skew_low/high as in backtest
            if trend == "down":
                majority = "token1"
                mode = "trend_down"
                pct_below_base = float(params.skew_high_pct)
                pct_above_base = float(params.skew_low_pct)
            else:
                majority = "token2"
                mode = "trend_up"
                pct_below_base = float(params.skew_low_pct)
                pct_above_base = float(params.skew_high_pct)

        if total_width_override is not None:
            total_width_pct = float(total_width_override)
        else:
            total_width_pct = float(params.high_vol_max_major_side_pct) if pool_type == "high_vol" else float(pct_below_base + pct_above_base)

        total_width_pct = max(float(total_width_pct), 2e-6)
        pct_below, pct_above = self._scale_to_total_width(pct_below_base, pct_above_base, total_width_pct)

        Pa = float(P) * (1.0 - float(pct_below))
        Pb = float(P) * (1.0 + float(pct_above))
        Pa, Pb = self._ensure_valid_band(Pa, Pb, float(P))
        return Pa, Pb, mode, majority, pct_below_base, pct_above_base

    # ==========================
    # Gates
    # ==========================
    
    @staticmethod
    def _update_breakout_streaks(
        P: float,
        Pa: float,
        Pb: float,
        eps: float,
        out_above_streak: int,
        out_below_streak: int,
        out_above_streak_total: int,
        out_below_streak_total: int,
    ) -> Tuple[int, int, int, int]:
        above = float(P) > float(Pb) * (1.0 + float(eps))
        below = float(P) < float(Pa) * (1.0 - float(eps))

        if above:
            return (
                int(out_above_streak) + 1,
                0,
                int(out_above_streak_total) + 1,
                int(out_below_streak_total),
            )
        if below:
            return (
                0,
                int(out_below_streak) + 1,
                int(out_above_streak_total),
                int(out_below_streak_total) + 1,
            )
        return 0, 0, int(out_above_streak_total), int(out_below_streak_total)

    @staticmethod
    def _gate_tier_runtime(
        tier: RangeTierParams,
        atr_now: Optional[float],
        trend: str,
        atr_streaks: Dict[str, int],
    ) -> bool:
        if atr_now is None:
            return False

        name = tier.name
        bars_req = int(tier.bars_required)

        thr = float(tier.atr_pct_threshold_down) if trend == "down" else float(tier.atr_pct_threshold)

        if float(atr_now) <= thr:
            atr_streaks[name] = int(atr_streaks.get(name, 0)) + 1
        else:
            atr_streaks[name] = 0

        return int(atr_streaks[name]) >= bars_req
    
    # ==========================
    # Main entrypoint
    # ==========================

    async def execute_for_indicator_snapshot(self, indicator_set: Dict, indicator_snapshot: Dict) -> None:
        symbol = indicator_snapshot["symbol"]

        P_signal = float(indicator_snapshot["close"])
        ema_f = float(indicator_snapshot["ema_fast"])
        ema_s = float(indicator_snapshot["ema_slow"])
        atr_pct = float(indicator_snapshot["atr_pct"])
        ts = int(indicator_snapshot["ts"])

        # NOTE: kept as-is from your prod code
        price = await self.market_data.get_token_price_usd(
            chain="base",
            token_address="0x4200000000000000000000000000000000000006",
        )
        P_exec = float(price["price_usd"])

        created_at_iso = indicator_snapshot.get("created_at_iso")
        if created_at_iso:
            if created_at_iso.endswith("Z"):
                created_at_iso = created_at_iso.replace("Z", "+00:00")
            dt_now = datetime.fromisoformat(created_at_iso)
        else:
            dt_now = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)

        strategies: List[StrategyEntity] = await self._strategy_repo.get_active_by_indicator_set(
            indicator_set_id=indicator_set["cfg_hash"]
        )
        if not strategies:
            return

        for strat in strategies:
            if strat.alias is None:
                continue

            params: StrategyParams = strat.params

            eps = float(params.eps)
            cooloff = int(params.cooloff_bars)
            breakout_confirm = int(params.breakout_confirm_bars)
            gauge_flow_enabled = bool(params.gauge_flow_enabled)

            trend_now = self._trend_at(ema_f, ema_s)

            strat_id = strat.name
            strat_stream_key = strat.stream_key

            current = await self._episode_repo.get_open_by_strategy(strat_id)

            # ==========================
            # Open first episode (ALWAYS high_vol DOWN)
            # ==========================
            if current is None:
                initial_pool_type = "high_vol"
                trend_for_pick = "down"
                width_override = float(params.high_vol_max_major_side_pct)

                Pa, Pb, mode, majority, pct_below_base, pct_above_base = await self._pick_band_for_trend_totalwidth(
                    P=P_exec,
                    trend=trend_for_pick,
                    params=params,
                    total_width_override=width_override,
                    pool_type=initial_pool_type,
                )

                # keep your existing convention
                if majority == "token1":
                    major_pct = pct_below_base * 10
                    minor_pct = pct_above_base * 10
                else:
                    major_pct = pct_above_base * 10
                    minor_pct = pct_below_base * 10

                # Persist band_params for pipeline parity (include NEW high-vol knobs)
                band_params = {
                    "skew_low_pct": float(params.skew_low_pct),
                    "skew_high_pct": float(params.skew_high_pct),

                    "high_vol_max_major_side_pct": float(params.high_vol_max_major_side_pct),

                    "high_vol_base_below_pct": float(params.high_vol_base_below_pct),
                    "high_vol_base_above_pct": float(params.high_vol_base_above_pct),
                    "high_vol_invert_on_trend_up": bool(params.high_vol_invert_on_trend_up),
                    "block_high_vol_up_atr_pct": float(params.block_high_vol_up_atr_pct) if params.block_high_vol_up_atr_pct is not None else None,

                    "tiers": [t.model_dump(mode="python") for t in (params.tiers or [])],
                }

                new_ep = StrategyEpisodeEntity(
                    id=f"ep_{strat_id}_{ts}",
                    stream_key=strat_stream_key,
                    strategy_id=strat_id,
                    symbol=symbol,
                    pool_type=initial_pool_type,
                    mode_on_open=mode,
                    majority_on_open=majority,
                    target_major_pct=major_pct,
                    target_minor_pct=minor_pct,
                    open_time=ts,
                    open_time_iso=indicator_snapshot.get("created_at_iso"),
                    open_price_signal=P_signal,
                    open_price_exec=P_exec,
                    Pa=Pa,
                    Pb=Pb,
                    band_total_width_pct=float(width_override),
                    band_params=band_params,
                    last_event_bar=0,
                    atr_streak={t.name: 0 for t in (params.tiers or [])},
                    out_above_streak=0,
                    out_below_streak=0,
                    out_above_streak_total=0,
                    out_below_streak_total=0,
                    dex=strat.dex,
                    alias=strat.alias,
                    token0_address=strat.token0,
                    token1_address=strat.token1,
                    gauge_flow_enabled=gauge_flow_enabled,
                )

                new_ep = await self._episode_repo.open_new(new_ep)

                signal_plan = await self._reconciler.reconcile(strat_id, new_ep, symbol)
                if signal_plan:
                    signal = SignalEntity(
                        strategy_id=strat_id,
                        indicator_set_id=indicator_set["cfg_hash"],
                        cfg_hash=indicator_set["cfg_hash"],
                        symbol=symbol,
                        ts=ts,
                        signal_type=SignalType(signal_plan["signal_type"]),
                        status=SignalStatus.PENDING,
                        attempts=0,
                        steps=[SignalStep(**step) for step in signal_plan["steps"]],
                        episode=new_ep,
                        last_episode=None,
                    )
                    await self._signal_repo.upsert_signal(signal)
                    if self._on_signal_created:
                        self._on_signal_created()
                continue

            # ==========================
            # Evaluate current episode
            # ==========================
            Pa_cur = float(current.Pa)
            Pb_cur = float(current.Pb)
            pool_type_cur = str(current.pool_type)

            i_since_open = int(current.last_event_bar) + 1

            out_above_streak = int(current.out_above_streak)
            out_below_streak = int(current.out_below_streak)
            out_above_streak_total = int(current.out_above_streak_total)
            out_below_streak_total = int(current.out_below_streak_total)

            atr_streaks: Dict[str, int] = dict(current.atr_streak or {})

            trigger: Optional[str] = None

            in_range_now = self._is_in_range(P_exec, Pa_cur, Pb_cur, eps)

            out_above_streak, out_below_streak, out_above_streak_total, out_below_streak_total = self._update_breakout_streaks(
                P_exec, Pa_cur, Pb_cur, eps,
                out_above_streak, out_below_streak,
                out_above_streak_total, out_below_streak_total,
            )

            # persist counters
            await self._episode_repo.update_partial(current.id, {
                "out_above_streak": out_above_streak,
                "out_below_streak": out_below_streak,
                "out_above_streak_total": out_above_streak_total,
                "out_below_streak_total": out_below_streak_total,
                "last_event_bar": i_since_open,
            })

            # 1) breakout trigger
            if out_above_streak >= breakout_confirm:
                trigger = "cross_max"
            elif out_below_streak >= breakout_confirm:
                trigger = "cross_min"

            # 2) tiers trigger (in-range + cooldown)
            if (not trigger) and in_range_now and (i_since_open >= cooloff):
                chosen_tier: Optional[RangeTierParams] = None
                for tier in reversed(params.tiers or []):
                    if pool_type_cur == tier.name:
                        break
                    if pool_type_cur not in (tier.allowed_from or []):
                        continue
                    if self._gate_tier_runtime(tier, atr_pct, trend_now, atr_streaks):
                        chosen_tier = tier
                        break

                await self._episode_repo.update_partial(current.id, {"atr_streak": atr_streaks})

                if chosen_tier:
                    trigger = f"tighten_{chosen_tier.name}"

            if not trigger:
                continue

            # ==========================
            # Close episode
            # ==========================
            now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            await self._episode_repo.close_episode(
                current.id,
                {
                    "close_time": ts,
                    "close_time_iso": now_iso,
                    "close_reason": trigger,
                    "close_price_exec": P_exec,
                    "close_price_signal": P_signal,
                },
            )

            current.close_time = ts
            current.close_time_iso = now_iso
            current.close_reason = trigger
            current.close_price_exec = P_exec
            current.close_price_signal = P_signal

            # ==========================
            # Decide next episode (backtest parity)
            # ==========================
            async def _open_episode(next_pool_type: str, width_override: Optional[float]) -> StrategyEpisodeEntity:
                # Trend for pick: trend_now, but BLOCK high_vol UP if ATR too high
                trend_for_pick = trend_now
                if next_pool_type == "high_vol" and trend_now == "up" and params.block_high_vol_up_atr_pct is not None:
                    if atr_pct is not None and float(atr_pct) > float(params.block_high_vol_up_atr_pct):
                        trend_for_pick = "down"

                total_width = float(width_override) if width_override is not None else float(params.high_vol_max_major_side_pct)
                total_width = max(total_width, 2e-6)

                Pa_new, Pb_new, mode_now, majority_now, pct_below_base, pct_above_base = await self._pick_band_for_trend_totalwidth(
                    P=P_exec,
                    trend=trend_for_pick,
                    params=params,
                    total_width_override=total_width,
                    pool_type=next_pool_type,
                )

                if majority_now == "token1":
                    major_pct = pct_below_base * 10
                    minor_pct = pct_above_base * 10
                else:
                    major_pct = pct_above_base * 10
                    minor_pct = pct_below_base * 10

                band_params = {
                    "skew_low_pct": float(params.skew_low_pct),
                    "skew_high_pct": float(params.skew_high_pct),

                    "high_vol_max_major_side_pct": float(params.high_vol_max_major_side_pct),

                    "high_vol_base_below_pct": float(params.high_vol_base_below_pct),
                    "high_vol_base_above_pct": float(params.high_vol_base_above_pct),
                    "high_vol_invert_on_trend_up": bool(params.high_vol_invert_on_trend_up),
                    "block_high_vol_up_atr_pct": float(params.block_high_vol_up_atr_pct) if params.block_high_vol_up_atr_pct is not None else None,

                    "tiers": [t.model_dump(mode="python") for t in (params.tiers or [])],
                }

                return StrategyEpisodeEntity(
                    id=f"ep_{strat_id}_{ts}",
                    strategy_id=strat_id,
                    stream_key=strat_stream_key,
                    symbol=symbol,
                    pool_type=next_pool_type,
                    mode_on_open=mode_now,
                    majority_on_open=majority_now,
                    target_major_pct=major_pct,
                    target_minor_pct=minor_pct,
                    open_time=ts,
                    open_time_iso=indicator_snapshot.get("created_at_iso"),
                    open_price_exec=P_exec,
                    open_price_signal=P_signal,
                    Pa=Pa_new,
                    Pb=Pb_new,
                    band_total_width_pct=total_width,
                    band_params=band_params,
                    last_event_bar=0,
                    atr_streak={t.name: 0 for t in (params.tiers or [])},
                    out_above_streak=0,
                    out_below_streak=0,
                    out_above_streak_total=0,
                    out_below_streak_total=0,
                    dex=strat.dex,
                    alias=strat.alias,
                    token0_address=strat.token0,
                    token1_address=strat.token1,
                    gauge_flow_enabled=gauge_flow_enabled,
                )

            new_ep: Optional[StrategyEpisodeEntity] = None

            if trigger in ("cross_min", "cross_max") or trigger.startswith("tighten_"):
                chosen_tier = None
                for tier in reversed(params.tiers or []):
                    if pool_type_cur not in (tier.allowed_from or []):
                        continue
                    if self._gate_tier_runtime(tier, atr_pct, trend_now, atr_streaks):
                        chosen_tier = tier
                        break

                await self._episode_repo.update_partial(current.id, {"atr_streak": atr_streaks})

                if chosen_tier:
                    new_ep = await _open_episode(chosen_tier.name, float(chosen_tier.max_major_side_pct))
                else:
                    new_ep = await _open_episode("high_vol", float(params.high_vol_max_major_side_pct))

            if not new_ep:
                continue

            await self._episode_repo.open_new(new_ep)

            signal_plan = await self._reconciler.reconcile(strat_id, new_ep, symbol)
            if signal_plan:
                signal = SignalEntity(
                    strategy_id=strat_id,
                    indicator_set_id=indicator_set["cfg_hash"],
                    cfg_hash=indicator_set["cfg_hash"],
                    symbol=symbol,
                    ts=ts,
                    signal_type=SignalType(signal_plan["signal_type"]),
                    status=SignalStatus.PENDING,
                    attempts=0,
                    steps=[SignalStep(**step) for step in signal_plan["steps"]],
                    episode=new_ep,
                    last_episode=current,
                )
                await self._signal_repo.upsert_signal(signal)
                if self._on_signal_created:
                    self._on_signal_created()