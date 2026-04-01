from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
from core.domain.entities.trade_strategy_runtime_snapshot_entity import (
    TradeStrategyRuntimeSnapshotEntity,
)
from core.domain.enums.trade_enums import (
    TradeAtrThresholdMode,
    TradeAtrThresholdSource,
    TradeEvent,
    TradeMode,
    TradePositionSide,
    TradeRuntimeState,
    TradeSignalType,
)
from core.services.trade_indicator_calculation_service import TradeIndicatorCalculationService


WEEKDAY_PT = {
    0: "segunda",
    1: "terça",
    2: "quarta",
    3: "quinta",
    4: "sexta",
    5: "sábado",
    6: "domingo",
}

WEEKDAY_PT_TO_INT = {v: k for k, v in WEEKDAY_PT.items()}


class AtrTwoStageTradeStrategyService:
    """
    Stateful ATR two-stage trade strategy evaluator.

    This service evaluates candles while preserving operational state across
    executions by using the previously persisted runtime snapshot.

    The runtime snapshot always represents the current strategy state after the
    latest processed candle, while the emitted signal represents only the action
    triggered by the latest closed candle.
    """

    def __init__(self, indicator_service: Optional[TradeIndicatorCalculationService] = None):
        """
        Initialize the strategy service.
        """
        self._indicator_service = indicator_service or TradeIndicatorCalculationService()

    def _compute_open_trade_loss_pct(
        self,
        *,
        position_side: Optional[TradePositionSide],
        entry_reference_price: Optional[float],
        current_price: float,
    ) -> float:
        """
        Compute the open-trade loss ratio against the stored entry reference price.

        This returns a fraction in the [0, +inf) domain, not a percentage:
        - LONG: loss when current price is below entry
        - SHORT: loss when current price is above entry
        """
        normalized_side = self._normalize_position_side(position_side)
        entry = float(entry_reference_price or 0.0)

        if normalized_side is None or entry <= 0.0:
            return 0.0

        if normalized_side == TradePositionSide.LONG:
            adverse_move = max(0.0, (entry - float(current_price)) / entry)
        else:
            adverse_move = max(0.0, (float(current_price) - entry) / entry)

        return float(adverse_move)

    def required_bars(self, strategy: TradeStrategyEntity) -> int:
        """
        Return the required candle window for evaluating the strategy safely.

        The required window now considers:
        - ATR warmup
        - optional dynamic ATR percentile windows
        - optional regime MA windows
        - cooloff / operational room
        """
        params = strategy.params

        need = max(
            self._indicator_service.required_bars_for_atr(atr_window=int(params.atr_window)),
            int(params.atr_window) + int(params.cooloff_bars) + 10,
        )

        if self._dynamic_atr_enabled(params):
            need = max(
                need,
                self._indicator_service.required_bars_for_atr(atr_window=int(params.atr_window))
                + self._indicator_service.required_bars_for_dynamic_threshold(
                    dynamic_window=int(params.atr_dynamic_window)
                )
                + 10,
            )

        if params.regime_trend_ma_window is not None:
            need = max(
                need,
                self._indicator_service.required_bars_for_ma(
                    ma_window=int(params.regime_trend_ma_window)
                )
                + 10,
            )

        if params.regime_structure_ma_window is not None:
            need = max(
                need,
                self._indicator_service.required_bars_for_ma(
                    ma_window=int(params.regime_structure_ma_window)
                )
                + 10,
            )

        return max(50, int(need))

    def evaluate_latest(
        self,
        *,
        strategy: TradeStrategyEntity,
        candles: Sequence[Dict[str, Any]],
        previous_snapshot: Optional[TradeStrategyRuntimeSnapshotEntity] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Evaluate the strategy on a candle window using the previous runtime state.

        Args:
            strategy: Strategy entity being evaluated.
            candles: Candle window in ascending or unsorted order.
            previous_snapshot: Previously persisted runtime snapshot used as
                the initial state for this evaluation.

        Returns:
            A dictionary with:
            - runtime_snapshot: latest strategy runtime state
            - signal: optional signal emitted on the latest processed candle

            Returns None when the candle window is unusable.
        """
        if not candles:
            return None

        data = pd.DataFrame(list(candles)).copy()
        if data.empty:
            return None

        required_cols = {"open_time", "close_time", "open", "high", "low", "close", "volume"}
        if not required_cols.issubset(set(data.columns)):
            return None

        data["open"] = pd.to_numeric(data["open"], errors="coerce")
        data["high"] = pd.to_numeric(data["high"], errors="coerce")
        data["low"] = pd.to_numeric(data["low"], errors="coerce")
        data["close"] = pd.to_numeric(data["close"], errors="coerce")
        data["volume"] = pd.to_numeric(data["volume"], errors="coerce")
        data["open_time"] = pd.to_numeric(data["open_time"], errors="coerce")
        data["close_time"] = pd.to_numeric(data["close_time"], errors="coerce")

        if "trades" not in data.columns:
            data["trades"] = 0
        data["trades"] = pd.to_numeric(data["trades"], errors="coerce").fillna(0).astype(int)

        if "is_closed" not in data.columns:
            data["is_closed"] = True

        data = data.dropna(subset=["open_time", "close_time", "open", "high", "low", "close", "volume"])
        data = data.sort_values("close_time").reset_index(drop=True)
        if data.empty:
            return None

        params = strategy.params

        atr_values, atr_pct_values = self._indicator_service.compute_atr_and_atr_pct(
            highs=data["high"].tolist(),
            lows=data["low"].tolist(),
            closes=data["close"].tolist(),
            window=int(params.atr_window),
        )
        if not atr_values or not atr_pct_values:
            return None

        data["atr"] = atr_values
        data["atr_pct"] = atr_pct_values

        atr_base_col = "atr" if params.atr_threshold_mode == TradeAtrThresholdMode.ATR else "atr_pct"
        atr_threshold_source = (
            TradeAtrThresholdSource.DYNAMIC
            if self._dynamic_atr_enabled(params)
            else TradeAtrThresholdSource.FIXED
        )

        if atr_threshold_source == TradeAtrThresholdSource.DYNAMIC:
            low_thr_values, high_thr_values = self._indicator_service.compute_dynamic_thresholds(
                values=data[atr_base_col].tolist(),
                window=int(params.atr_dynamic_window),
                low_percentile=float(params.atr_dynamic_low_percentile),
                high_percentile=float(params.atr_dynamic_high_percentile),
                min_periods=params.atr_dynamic_min_periods,
            )
            data["atr_low_threshold_active"] = low_thr_values
            data["atr_high_threshold_active"] = high_thr_values
        else:
            data["atr_low_threshold_active"] = float(params.atr_low_threshold)
            data["atr_high_threshold_active"] = float(params.atr_high_threshold)

        data["regime_trend_ma"] = self._indicator_service.compute_ma(
            values=data["close"].tolist(),
            window=params.regime_trend_ma_window,
            ma_type=params.regime_trend_ma_type,
        )
        data["regime_structure_ma"] = self._indicator_service.compute_ma(
            values=data["close"].tolist(),
            window=params.regime_structure_ma_window,
            ma_type=params.regime_structure_ma_type,
        )

        if previous_snapshot is None:
            indices_to_process = list(range(1, len(data)))
            setup_armed = False
            setup_reference_price: Optional[float] = None
            position_side: Optional[TradePositionSide] = None
            entry_reference_price: Optional[float] = None
            bars_since_last_event = 1000000
        else:
            previous_ts = int(previous_snapshot.close_time)
            indices_to_process = [
                int(idx)
                for idx in data.index.tolist()
                if int(data.loc[idx, "close_time"]) > previous_ts
            ]

            setup_armed = bool(previous_snapshot.setup_armed)
            setup_reference_price = (
                float(previous_snapshot.setup_reference_price)
                if previous_snapshot.setup_reference_price is not None
                else None
            )
            position_side = self._normalize_position_side(previous_snapshot.position_side)
            entry_reference_price = (
                float(previous_snapshot.entry_reference_price)
                if getattr(previous_snapshot, "entry_reference_price", None) is not None
                else None
            )
            bars_since_last_event = int(getattr(previous_snapshot, "bars_since_last_event", 1000000) or 1000000)

        if not indices_to_process:
            if previous_snapshot is None:
                return None
            return {
                "runtime_snapshot": previous_snapshot.model_dump(mode="python"),
                "signal": None,
            }

        allowed_days = self._normalize_allowed_weekdays(params.allowed_weekdays)

        latest_runtime_snapshot: Optional[Dict[str, Any]] = None
        latest_signal: Optional[Dict[str, Any]] = None

        for idx in indices_to_process:
            bars_since_last_event += 1

            candle_open = float(data.loc[idx, "open"])
            candle_high = float(data.loc[idx, "high"])
            candle_low = float(data.loc[idx, "low"])
            candle_close = float(data.loc[idx, "close"])
            candle_volume = float(data.loc[idx, "volume"])
            candle_open_time = int(data.loc[idx, "open_time"])
            candle_close_time = int(data.loc[idx, "close_time"])
            candle_trades = int(data.loc[idx, "trades"])
            candle_is_closed = bool(data.loc[idx, "is_closed"])

            atr_now = float(data.loc[idx, "atr"])
            atr_pct_now = float(data.loc[idx, "atr_pct"])
            atr_value_for_threshold = float(data.loc[idx, atr_base_col])

            atr_low_threshold_active = self._as_optional_float(data.loc[idx, "atr_low_threshold_active"])
            atr_high_threshold_active = self._as_optional_float(data.loc[idx, "atr_high_threshold_active"])

            low_atr_hit = int(
                atr_low_threshold_active is not None
                and atr_value_for_threshold <= float(atr_low_threshold_active)
            )
            high_atr_hit = int(
                atr_high_threshold_active is not None
                and atr_value_for_threshold >= float(atr_high_threshold_active)
            )

            desired_side: Optional[TradePositionSide] = None
            if setup_armed:
                desired_side = self._resolve_desired_side(
                    current_price=candle_close,
                    reference_price=setup_reference_price,
                    reverse_signal=bool(params.reverse_signal),
                    trade_mode=params.trade_mode,
                )

            regime_trend_ma_now = self._as_optional_float(data.loc[idx, "regime_trend_ma"])
            regime_structure_ma_now = self._as_optional_float(data.loc[idx, "regime_structure_ma"])

            regime_allows_long = self._regime_allows_side(
                side=TradePositionSide.LONG,
                price=candle_close,
                trend_ma_value=regime_trend_ma_now,
                structure_ma_value=regime_structure_ma_now,
                strategy=strategy,
            )
            regime_allows_short = self._regime_allows_side(
                side=TradePositionSide.SHORT,
                price=candle_close,
                trend_ma_value=regime_trend_ma_now,
                structure_ma_value=regime_structure_ma_now,
                strategy=strategy,
            )
            regime_allows_desired = (
                self._regime_allows_side(
                    side=desired_side,
                    price=candle_close,
                    trend_ma_value=regime_trend_ma_now,
                    structure_ma_value=regime_structure_ma_now,
                    strategy=strategy,
                )
                if desired_side is not None
                else None
            )

            open_trade_loss_pct_before_event = self._compute_open_trade_loss_pct(
                position_side=position_side,
                entry_reference_price=entry_reference_price,
                current_price=candle_close,
            )

            can_fire = int(bars_since_last_event) > int(max(0, params.cooloff_bars))
            is_allowed_day = self._is_allowed_day(candle_close_time, allowed_days)

            signal_up = 0
            signal_down = 0
            signal_up_first = 0
            signal_down_first = 0
            exit_signal = 0
            event: Optional[TradeEvent] = None
            signal_for_candle: Optional[Dict[str, Any]] = None

            if position_side is not None:
                if (
                    params.max_loss_pct is not None
                    and open_trade_loss_pct_before_event >= float(params.max_loss_pct)
                    and can_fire
                ):
                    closed_side = position_side
                    closed_entry_reference_price = entry_reference_price

                    exit_signal = 1
                    event = TradeEvent.MAX_LOSS_STOP

                    position_side = None
                    entry_reference_price = None
                    setup_armed = False
                    setup_reference_price = None
                    bars_since_last_event = 0

                    if closed_side == TradePositionSide.LONG:
                        close_signal_type = TradeSignalType.CLOSE_LONG
                    elif closed_side == TradePositionSide.SHORT:
                        close_signal_type = TradeSignalType.CLOSE_SHORT
                    else:
                        close_signal_type = TradeSignalType.CLOSE_POSITION

                    signal_for_candle = {
                        "signal_type": close_signal_type,
                        "ts": candle_close_time,
                        "close": candle_close,
                        "event": event,
                        "position_side": closed_side,
                        "desired_side": desired_side,
                        "atr": atr_now,
                        "atr_pct": atr_pct_now,
                        "atr_threshold_source": atr_threshold_source,
                        "atr_low_threshold_active": atr_low_threshold_active,
                        "atr_high_threshold_active": atr_high_threshold_active,
                        "regime_trend_ma": regime_trend_ma_now,
                        "regime_structure_ma": regime_structure_ma_now,
                        "regime_allows_desired": regime_allows_desired,
                        "setup_reference_price": setup_reference_price,
                        "entry_reference_price": closed_entry_reference_price,
                        "open_trade_loss_pct": float(open_trade_loss_pct_before_event),
                        "runtime_state": self._resolve_runtime_state(
                            setup_armed=setup_armed,
                            position_side=position_side,
                        ),
                    }

                elif low_atr_hit and can_fire:
                    closed_side = position_side
                    closed_entry_reference_price = entry_reference_price

                    exit_signal = 1
                    event = TradeEvent.ATR_LOW_EXIT

                    position_side = None
                    entry_reference_price = None
                    setup_armed = True
                    setup_reference_price = candle_close
                    bars_since_last_event = 0

                    if closed_side == TradePositionSide.LONG:
                        close_signal_type = TradeSignalType.CLOSE_LONG
                    elif closed_side == TradePositionSide.SHORT:
                        close_signal_type = TradeSignalType.CLOSE_SHORT
                    else:
                        close_signal_type = TradeSignalType.CLOSE_POSITION

                    signal_for_candle = {
                        "signal_type": close_signal_type,
                        "ts": candle_close_time,
                        "close": candle_close,
                        "event": event,
                        "position_side": closed_side,
                        "desired_side": desired_side,
                        "atr": atr_now,
                        "atr_pct": atr_pct_now,
                        "atr_threshold_source": atr_threshold_source,
                        "atr_low_threshold_active": atr_low_threshold_active,
                        "atr_high_threshold_active": atr_high_threshold_active,
                        "regime_trend_ma": regime_trend_ma_now,
                        "regime_structure_ma": regime_structure_ma_now,
                        "regime_allows_desired": regime_allows_desired,
                        "setup_reference_price": setup_reference_price,
                        "entry_reference_price": closed_entry_reference_price,
                        "open_trade_loss_pct": float(open_trade_loss_pct_before_event),
                        "runtime_state": self._resolve_runtime_state(
                            setup_armed=setup_armed,
                            position_side=position_side,
                        ),
                    }

            else:
                if low_atr_hit:
                    setup_armed = True
                    setup_reference_price = candle_close
                    event = TradeEvent.ARM_ON_LOW_ATR

                elif setup_armed and high_atr_hit and desired_side is not None and is_allowed_day and can_fire:
                    if regime_allows_desired is False:
                        event = (
                            TradeEvent.REGIME_BLOCKED_LONG
                            if desired_side == TradePositionSide.LONG
                            else TradeEvent.REGIME_BLOCKED_SHORT
                        )
                    else:
                        opened_side = desired_side

                        if opened_side == TradePositionSide.LONG:
                            signal_up = 1
                            signal_up_first = 1
                            event = TradeEvent.OPEN_LONG
                        else:
                            signal_down = 1
                            signal_down_first = 1
                            event = TradeEvent.OPEN_SHORT

                        position_side = opened_side
                        entry_reference_price = candle_close
                        setup_armed = False
                        setup_reference_price = None
                        bars_since_last_event = 0

                        signal_for_candle = {
                            "signal_type": TradeSignalType(event.value),
                            "ts": candle_close_time,
                            "close": candle_close,
                            "event": event,
                            "position_side": opened_side,
                            "desired_side": desired_side,
                            "atr": atr_now,
                            "atr_pct": atr_pct_now,
                            "atr_threshold_source": atr_threshold_source,
                            "atr_low_threshold_active": atr_low_threshold_active,
                            "atr_high_threshold_active": atr_high_threshold_active,
                            "regime_trend_ma": regime_trend_ma_now,
                            "regime_structure_ma": regime_structure_ma_now,
                            "regime_allows_desired": regime_allows_desired,
                            "setup_reference_price": setup_reference_price,
                            "entry_reference_price": entry_reference_price,
                            "open_trade_loss_pct": 0.0,
                            "runtime_state": self._resolve_runtime_state(
                                setup_armed=setup_armed,
                                position_side=position_side,
                            ),
                        }

            runtime_open_trade_loss_pct = self._compute_open_trade_loss_pct(
                position_side=position_side,
                entry_reference_price=entry_reference_price,
                current_price=candle_close,
            )

            runtime_state = self._resolve_runtime_state(
                setup_armed=setup_armed,
                position_side=position_side,
            )

            latest_runtime_snapshot = {
                "strategy_id": str(strategy.id),
                "stream_key": str(strategy.stream_key),
                "symbol": str(strategy.symbol).upper(),
                "interval": str(strategy.interval).lower(),
                "strategy_type": strategy.strategy_type,
                "ts": candle_close_time,
                "open_time": candle_open_time,
                "close_time": candle_close_time,
                "open": candle_open,
                "high": candle_high,
                "low": candle_low,
                "close": candle_close,
                "volume": candle_volume,
                "trades": candle_trades,
                "is_closed": candle_is_closed,
                "atr": atr_now,
                "atr_pct": atr_pct_now,
                "atr_value_for_threshold": atr_value_for_threshold,
                "atr_threshold_source": atr_threshold_source,
                "atr_low_threshold_active": atr_low_threshold_active,
                "atr_high_threshold_active": atr_high_threshold_active,
                "low_atr_hit": int(low_atr_hit),
                "high_atr_hit": int(high_atr_hit),
                "setup_armed": int(setup_armed),
                "setup_reference_price": setup_reference_price,
                "desired_side": desired_side,
                "position_side": position_side,
                "regime_trend_ma": regime_trend_ma_now,
                "regime_structure_ma": regime_structure_ma_now,
                "regime_allows_long": int(regime_allows_long),
                "regime_allows_short": int(regime_allows_short),
                "regime_allows_desired": (
                    int(regime_allows_desired)
                    if regime_allows_desired is not None
                    else None
                ),
                "entry_reference_price": entry_reference_price,
                "open_trade_loss_pct": float(runtime_open_trade_loss_pct),
                "event": event,
                "signal_up": int(signal_up),
                "signal_down": int(signal_down),
                "signal_up_first": int(signal_up_first),
                "signal_down_first": int(signal_down_first),
                "exit_signal": int(exit_signal),
                "runtime_state": runtime_state,
                "bars_since_last_event": int(bars_since_last_event),
            }

            latest_signal = signal_for_candle

        if latest_runtime_snapshot is None:
            return None

        return {
            "runtime_snapshot": latest_runtime_snapshot,
            "signal": latest_signal,
        }

    def _dynamic_atr_enabled(self, params) -> bool:
        """
        Check whether dynamic ATR thresholds are enabled.
        """
        dynamic_core = [
            params.atr_dynamic_window,
            params.atr_dynamic_low_percentile,
            params.atr_dynamic_high_percentile,
        ]
        return all(x is not None for x in dynamic_core)

    def _resolve_desired_side(
        self,
        *,
        current_price: float,
        reference_price: Optional[float],
        reverse_signal: bool,
        trade_mode: TradeMode,
    ) -> Optional[TradePositionSide]:
        """
        Resolve the desired trade side from the setup reference price.
        """
        if reference_price is None:
            return None

        desired_side: Optional[TradePositionSide] = None
        if float(current_price) > float(reference_price):
            desired_side = TradePositionSide.LONG
        elif float(current_price) < float(reference_price):
            desired_side = TradePositionSide.SHORT

        if desired_side is None:
            return None

        if reverse_signal:
            desired_side = (
                TradePositionSide.SHORT
                if desired_side == TradePositionSide.LONG
                else TradePositionSide.LONG
            )

        if desired_side == TradePositionSide.LONG:
            return desired_side if trade_mode in {TradeMode.FLIP, TradeMode.LONG_ONLY, TradeMode.FLAT_ON_DOWN} else None

        if desired_side == TradePositionSide.SHORT:
            return desired_side if trade_mode in {TradeMode.FLIP, TradeMode.SHORT_ONLY} else None

        return None

    def _side_allowed_by_ma(
        self,
        *,
        side: TradePositionSide,
        price: float,
        ma_value: Optional[float],
        reverse: bool = False,
    ) -> bool:
        """
        Check if a side is allowed by a single regime MA.
        """
        if ma_value is None or pd.isna(ma_value):
            return False

        if reverse is False:
            if side == TradePositionSide.LONG:
                return float(price) >= float(ma_value)
            return float(price) <= float(ma_value)

        if side == TradePositionSide.LONG:
            return float(price) < float(ma_value)
        return float(price) > float(ma_value)

    def _regime_allows_side(
        self,
        *,
        side: Optional[TradePositionSide],
        price: float,
        trend_ma_value: Optional[float],
        structure_ma_value: Optional[float],
        strategy: TradeStrategyEntity,
    ) -> bool:
        """
        Check whether the optional regime filters allow the given side.

        Legacy behavior is preserved:
        - if both MA windows are None, both LONG and SHORT remain allowed
        """
        if side is None:
            return False

        params = strategy.params
        reverse = bool(params.regime_reverse)

        trend_ok = True
        if params.regime_trend_ma_window is not None:
            trend_ok = self._side_allowed_by_ma(
                side=side,
                price=price,
                ma_value=trend_ma_value,
                reverse=reverse,
            )

        structure_ok = True
        if params.regime_structure_ma_window is not None:
            structure_ok = self._side_allowed_by_ma(
                side=side,
                price=price,
                ma_value=structure_ma_value,
                reverse=reverse,
            )

        return bool(trend_ok and structure_ok)

    def _normalize_allowed_weekdays(self, value: Optional[List[int | str]]) -> Optional[set[int]]:
        """
        Normalize weekday names in Portuguese or integers into weekday integers.
        """
        if value is None:
            return None

        out: set[int] = set()
        for item in value:
            if isinstance(item, int):
                if 0 <= int(item) <= 6:
                    out.add(int(item))
                continue

            raw = str(item).strip().lower()
            if not raw:
                continue

            if raw in WEEKDAY_PT_TO_INT:
                out.add(WEEKDAY_PT_TO_INT[raw])
                continue

            for name, idx in WEEKDAY_PT_TO_INT.items():
                if name.startswith(raw):
                    out.add(idx)
                    break

        return out if out else None

    def _is_allowed_day(self, candle_ts_ms: int, allowed_days: Optional[set[int]]) -> bool:
        """
        Check whether a candle timestamp is allowed by weekday filters.
        """
        if allowed_days is None:
            return True

        dt = pd.to_datetime(int(candle_ts_ms), unit="ms", utc=True, errors="coerce")
        if pd.isna(dt):
            return False

        return int(dt.weekday()) in allowed_days

    def _normalize_position_side(self, value: Optional[TradePositionSide | str]) -> Optional[TradePositionSide]:
        """
        Normalize a stored position side value.
        """
        if value is None:
            return None

        if isinstance(value, TradePositionSide):
            return value

        raw = str(value).strip().upper()
        if raw == TradePositionSide.LONG.value:
            return TradePositionSide.LONG
        if raw == TradePositionSide.SHORT.value:
            return TradePositionSide.SHORT
        return None

    def _resolve_runtime_state(
        self,
        *,
        setup_armed: bool,
        position_side: Optional[TradePositionSide],
    ) -> TradeRuntimeState:
        """
        Resolve the current operational state enum.
        """
        normalized_side = self._normalize_position_side(position_side)
        if normalized_side == TradePositionSide.LONG:
            return TradeRuntimeState.OPEN_LONG
        if normalized_side == TradePositionSide.SHORT:
            return TradeRuntimeState.OPEN_SHORT
        if setup_armed:
            return TradeRuntimeState.ARMED
        return TradeRuntimeState.FLAT

    def _as_optional_float(self, value: Any) -> Optional[float]:
        """
        Convert values to float while preserving missing values.
        """
        try:
            if value is None or pd.isna(value):
                return None
            return float(value)
        except Exception:
            return None