from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
from core.domain.entities.trade_strategy_runtime_snapshot_entity import (
    TradeStrategyRuntimeSnapshotEntity,
)
from core.domain.enums.trade_enums import (
    TradeAtrThresholdMode,
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

    def required_bars(self, strategy: TradeStrategyEntity) -> int:
        """
        Return the required candle window for evaluating the strategy safely.
        """
        params = strategy.params
        return max(
            self._indicator_service.required_bars_for_atr(atr_window=int(params.atr_window)),
            int(params.atr_window) + int(params.cooloff_bars) + 10,
        )

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

        if previous_snapshot is None:
            indices_to_process = list(range(1, len(data)))
            setup_armed = False
            setup_reference_price: Optional[float] = None
            position_side: Optional[TradePositionSide] = None
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
            atr_value_for_threshold = float(
                atr_now if params.atr_threshold_mode == TradeAtrThresholdMode.ATR else atr_pct_now
            )

            low_atr_hit = int(atr_value_for_threshold <= float(params.atr_low_threshold))
            high_atr_hit = int(atr_value_for_threshold >= float(params.atr_high_threshold))

            desired_side: Optional[TradePositionSide] = None
            if setup_armed:
                desired_side = self._resolve_desired_side(
                    current_price=candle_close,
                    reference_price=setup_reference_price,
                    reverse_signal=bool(params.reverse_signal),
                    trade_mode=params.trade_mode,
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
                if low_atr_hit and can_fire:
                    closed_side = position_side

                    exit_signal = 1
                    event = TradeEvent.ATR_LOW_EXIT

                    position_side = None
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
                        "atr": atr_now,
                        "atr_pct": atr_pct_now,
                        "setup_reference_price": setup_reference_price,
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
                    setup_armed = False
                    setup_reference_price = None
                    bars_since_last_event = 0

                    signal_for_candle = {
                        "signal_type": TradeSignalType(event.value),
                        "ts": candle_close_time,
                        "close": candle_close,
                        "event": event,
                        "position_side": opened_side,
                        "atr": atr_now,
                        "atr_pct": atr_pct_now,
                        "setup_reference_price": setup_reference_price,
                        "runtime_state": self._resolve_runtime_state(
                            setup_armed=setup_armed,
                            position_side=position_side,
                        ),
                    }

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
                "low_atr_hit": int(low_atr_hit),
                "high_atr_hit": int(high_atr_hit),
                "setup_armed": int(setup_armed),
                "setup_reference_price": setup_reference_price,
                "desired_side": desired_side,
                "position_side": position_side,
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

    def _normalize_allowed_weekdays(self, value: Optional[List[str]]) -> Optional[set[int]]:
        """
        Normalize weekday names in Portuguese into weekday integers.
        """
        if value is None:
            return None

        out: set[int] = set()
        for item in value:
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