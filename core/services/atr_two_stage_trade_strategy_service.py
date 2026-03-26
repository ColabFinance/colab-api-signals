from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity
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
    Trade strategy service for ATR two-stage evaluation.

    This service evaluates a full candle window and returns two outputs:
    - a runtime snapshot for the latest candle
    - an optional signal when the latest candle triggers an action

    The runtime snapshot is persisted even when no signal is emitted.
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
    ) -> Optional[Dict[str, Any]]:
        """
        Evaluate the strategy on a candle window.

        Returns:
            A dictionary with:
            - runtime_snapshot: the latest computed strategy state
            - signal: the optional signal emitted on the latest candle only

            Returns None when the candle window is not usable.
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

        n = len(data)

        signal_up = [0] * n
        signal_down = [0] * n
        signal_up_first = [0] * n
        signal_down_first = [0] * n
        exit_signal = [0] * n

        low_atr_hit_col = [0] * n
        high_atr_hit_col = [0] * n
        setup_armed_col = [0] * n

        event_col: List[Optional[str]] = [None] * n
        desired_side_col: List[Optional[str]] = [None] * n
        position_side_col: List[Optional[str]] = [None] * n
        setup_reference_price_col: List[Optional[float]] = [None] * n

        setup_armed = False
        setup_reference_price: Optional[float] = None
        position_side: Optional[str] = None
        last_event_bar = -100000

        allowed_days = self._normalize_allowed_weekdays(params.allowed_weekdays)

        for idx in range(1, n):
            candle_close = float(data.loc[idx, "close"])
            candle_ts = int(data.loc[idx, "close_time"])

            atr_value_for_threshold = float(
                data.loc[idx, "atr"] if params.atr_threshold_mode == "atr" else data.loc[idx, "atr_pct"]
            )

            low_atr_hit = atr_value_for_threshold <= float(params.atr_low_threshold)
            high_atr_hit = atr_value_for_threshold >= float(params.atr_high_threshold)

            low_atr_hit_col[idx] = int(low_atr_hit)
            high_atr_hit_col[idx] = int(high_atr_hit)

            desired_side = None
            if setup_armed:
                desired_side = self._resolve_desired_side(
                    current_price=candle_close,
                    reference_price=setup_reference_price,
                    reverse_signal=bool(params.reverse_signal),
                    trade_mode=str(params.trade_mode),
                )
            desired_side_col[idx] = desired_side

            can_fire = (idx - last_event_bar) > int(max(0, params.cooloff_bars))
            is_allowed_day = self._is_allowed_day(candle_ts, allowed_days)

            if position_side is not None:
                if low_atr_hit and can_fire:
                    exit_signal[idx] = 1
                    event_col[idx] = "ATR_LOW_EXIT"

                    position_side = None
                    setup_armed = True
                    setup_reference_price = candle_close
                    last_event_bar = idx

            else:
                if low_atr_hit:
                    setup_armed = True
                    setup_reference_price = candle_close
                    event_col[idx] = "ARM_ON_LOW_ATR"

                elif setup_armed and high_atr_hit and desired_side is not None and is_allowed_day and can_fire:
                    if desired_side == "long":
                        signal_up[idx] = 1
                        signal_up_first[idx] = 1
                        event_col[idx] = "OPEN_LONG"
                    else:
                        signal_down[idx] = 1
                        signal_down_first[idx] = 1
                        event_col[idx] = "OPEN_SHORT"

                    position_side = desired_side
                    setup_armed = False
                    setup_reference_price = None
                    last_event_bar = idx

            setup_armed_col[idx] = int(setup_armed)
            setup_reference_price_col[idx] = setup_reference_price
            position_side_col[idx] = position_side

        last_idx = n - 1
        last_open = float(data.loc[last_idx, "open"])
        last_high = float(data.loc[last_idx, "high"])
        last_low = float(data.loc[last_idx, "low"])
        last_close = float(data.loc[last_idx, "close"])
        last_volume = float(data.loc[last_idx, "volume"])
        last_open_time = int(data.loc[last_idx, "open_time"])
        last_close_time = int(data.loc[last_idx, "close_time"])
        last_trades = int(data.loc[last_idx, "trades"])
        last_is_closed = bool(data.loc[last_idx, "is_closed"])
        last_atr = float(data.loc[last_idx, "atr"])
        last_atr_pct = float(data.loc[last_idx, "atr_pct"])
        last_atr_value_for_threshold = float(last_atr if params.atr_threshold_mode == "atr" else last_atr_pct)

        runtime_snapshot = {
            "strategy_id": str(strategy.id),
            "stream_key": str(strategy.stream_key),
            "symbol": str(strategy.symbol).upper(),
            "interval": str(strategy.interval).lower(),
            "strategy_type": str(strategy.strategy_type).lower(),
            "ts": last_close_time,
            "open_time": last_open_time,
            "close_time": last_close_time,
            "open": last_open,
            "high": last_high,
            "low": last_low,
            "close": last_close,
            "volume": last_volume,
            "trades": last_trades,
            "is_closed": last_is_closed,
            "atr": last_atr,
            "atr_pct": last_atr_pct,
            "atr_value_for_threshold": last_atr_value_for_threshold,
            "low_atr_hit": int(low_atr_hit_col[last_idx]),
            "high_atr_hit": int(high_atr_hit_col[last_idx]),
            "setup_armed": int(setup_armed_col[last_idx]),
            "setup_reference_price": (
                float(setup_reference_price_col[last_idx])
                if setup_reference_price_col[last_idx] is not None
                else None
            ),
            "desired_side": desired_side_col[last_idx],
            "position_side": position_side_col[last_idx],
            "event": event_col[last_idx],
            "signal_up": int(signal_up[last_idx]),
            "signal_down": int(signal_down[last_idx]),
            "signal_up_first": int(signal_up_first[last_idx]),
            "signal_down_first": int(signal_down_first[last_idx]),
            "exit_signal": int(exit_signal[last_idx]),
        }

        signal: Optional[Dict[str, Any]] = None
        if runtime_snapshot["signal_up"] == 1:
            signal = {
                "signal_type": "OPEN_LONG",
                "ts": runtime_snapshot["ts"],
                "close": runtime_snapshot["close"],
                "event": runtime_snapshot["event"],
                "position_side": "LONG",
                "atr": runtime_snapshot["atr"],
                "atr_pct": runtime_snapshot["atr_pct"],
                "setup_reference_price": runtime_snapshot["setup_reference_price"],
            }
        elif runtime_snapshot["signal_down"] == 1:
            signal = {
                "signal_type": "OPEN_SHORT",
                "ts": runtime_snapshot["ts"],
                "close": runtime_snapshot["close"],
                "event": runtime_snapshot["event"],
                "position_side": "SHORT",
                "atr": runtime_snapshot["atr"],
                "atr_pct": runtime_snapshot["atr_pct"],
                "setup_reference_price": runtime_snapshot["setup_reference_price"],
            }
        elif runtime_snapshot["exit_signal"] == 1:
            side = runtime_snapshot["desired_side"] or runtime_snapshot["position_side"] or "UNKNOWN"
            if str(side).upper() == "LONG":
                close_type = "CLOSE_LONG"
            elif str(side).upper() == "SHORT":
                close_type = "CLOSE_SHORT"
            else:
                close_type = "CLOSE_POSITION"

            signal = {
                "signal_type": close_type,
                "ts": runtime_snapshot["ts"],
                "close": runtime_snapshot["close"],
                "event": runtime_snapshot["event"],
                "position_side": str(side).upper(),
                "atr": runtime_snapshot["atr"],
                "atr_pct": runtime_snapshot["atr_pct"],
                "setup_reference_price": runtime_snapshot["setup_reference_price"],
            }

        return {
            "runtime_snapshot": runtime_snapshot,
            "signal": signal,
        }

    def _resolve_desired_side(
        self,
        *,
        current_price: float,
        reference_price: Optional[float],
        reverse_signal: bool,
        trade_mode: str,
    ) -> Optional[str]:
        """
        Resolve the desired trade side from the setup reference price.
        """
        if reference_price is None:
            return None

        desired_side: Optional[str] = None
        if float(current_price) > float(reference_price):
            desired_side = "long"
        elif float(current_price) < float(reference_price):
            desired_side = "short"

        if desired_side is None:
            return None

        if reverse_signal:
            desired_side = "short" if desired_side == "long" else "long"

        if desired_side == "long":
            return "long" if trade_mode in ("flip", "long_only", "flat_on_down") else None

        if desired_side == "short":
            return "short" if trade_mode in ("flip", "short_only") else None

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