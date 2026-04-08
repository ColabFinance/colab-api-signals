from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from core.domain.entities.lp_indicator_snapshot_entity import LpIndicatorSnapshotEntity


class LpIndicatorCalculationService:
    """
    Pure calculation service for LP indicators inside api-signals.
    """

    def required_bars_for(
        self,
        *,
        atr_window: int,
        entry_trend_ma_window: int,
    ) -> int:
        atr_need = int(atr_window) + 1
        trend_need = int(entry_trend_ma_window) + 1 if int(entry_trend_ma_window or 0) > 0 else 0
        return max(atr_need, trend_need)

    def compute_snapshot_for_last(
        self,
        *,
        candles: Sequence[Dict[str, Any]],
        atr_window: int,
        entry_trend_ma_window: int,
    ) -> Optional[LpIndicatorSnapshotEntity]:
        if not candles:
            return None

        ordered = sorted(list(candles), key=lambda x: int(x.get("close_time") or 0))
        need = self.required_bars_for(
            atr_window=int(atr_window),
            entry_trend_ma_window=int(entry_trend_ma_window or 0),
        )
        if len(ordered) < need:
            return None

        closes = [float(c["close"]) for c in ordered]
        highs = [float(c["high"]) for c in ordered]
        lows = [float(c["low"]) for c in ordered]

        atr = self._atr(highs, lows, closes, int(atr_window))
        if atr is None:
            return None

        entry_trend_ma = None
        entry_trend_ma_prev = None
        entry_trend_ma_distance_pct = None
        entry_trend_ma_slope_pct = None

        if int(entry_trend_ma_window or 0) > 0:
            entry_trend_ma = self._ema(closes, int(entry_trend_ma_window))
            entry_trend_ma_prev = self._ema(closes[:-1], int(entry_trend_ma_window))

            last_close = float(closes[-1])

            if entry_trend_ma is not None and entry_trend_ma > 0:
                entry_trend_ma_distance_pct = abs((last_close / float(entry_trend_ma)) - 1.0)

            if (
                entry_trend_ma is not None
                and entry_trend_ma_prev is not None
                and float(entry_trend_ma_prev) > 0
            ):
                entry_trend_ma_slope_pct = abs(
                    (float(entry_trend_ma) / float(entry_trend_ma_prev)) - 1.0
                )

        last = ordered[-1]
        close = float(last["close"])
        atr_pct = float(atr) / close if close > 0 else 0.0

        stream_key = str(last.get("stream_key") or "").strip().lower()
        source = str(last.get("source") or "").strip().lower()
        if not source and stream_key:
            source = str(stream_key.split(":")[0]).strip().lower()

        symbol = str(last.get("symbol") or "").strip().upper()
        if not symbol and stream_key:
            parts = stream_key.split(":")
            if len(parts) >= 2:
                symbol = str(parts[1]).strip().upper()

        interval = str(last.get("interval") or "").strip().lower()
        if not interval and stream_key:
            parts = stream_key.split(":")
            if len(parts) >= 3:
                interval = str(parts[2]).strip().lower()

        created_at_iso = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

        return LpIndicatorSnapshotEntity(
            stream_key=stream_key,
            source=source,
            symbol=symbol,
            interval=interval,
            ts=int(last.get("close_time")),
            open_time=int(last.get("open_time")),
            close_time=int(last.get("close_time")),
            open=float(last.get("open")),
            high=float(last.get("high")),
            low=float(last.get("low")),
            close=float(close),
            atr=float(atr),
            atr_pct=float(atr_pct),
            entry_trend_ma=(float(entry_trend_ma) if entry_trend_ma is not None else None),
            entry_trend_ma_prev=(float(entry_trend_ma_prev) if entry_trend_ma_prev is not None else None),
            entry_trend_ma_distance_pct=(
                float(entry_trend_ma_distance_pct)
                if entry_trend_ma_distance_pct is not None
                else None
            ),
            entry_trend_ma_slope_pct=(
                float(entry_trend_ma_slope_pct)
                if entry_trend_ma_slope_pct is not None
                else None
            ),
            created_at_iso=created_at_iso,
        )

    def _ema(self, values: List[float], period: int) -> Optional[float]:
        if period <= 0 or len(values) < period:
            return None
        k = 2.0 / (period + 1.0)
        ema = sum(values[:period]) / float(period)
        for v in values[period:]:
            ema = (float(v) - ema) * k + ema
        return float(ema)

    def _atr(self, highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
        if period <= 0 or len(closes) < period + 1:
            return None
        trs: List[float] = []
        for i in range(1, len(closes)):
            h = float(highs[i])
            l = float(lows[i])
            prev_c = float(closes[i - 1])
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
        window = trs[-period:]
        return float(sum(window) / float(period))