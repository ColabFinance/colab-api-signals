from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence


class LpIndicatorCalculationService:
    """
    Pure calculation service for LP indicators inside api-signals.
    """

    def required_bars_for(self, *, ema_slow: int, atr_window: int) -> int:
        ema_need = int(ema_slow)
        atr_need = int(atr_window) + 1
        return max(ema_need, atr_need)

    def compute_snapshot_for_last(
        self,
        *,
        candles: Sequence[Dict[str, Any]],
        ema_fast: int,
        ema_slow: int,
        atr_window: int,
    ) -> Optional[Dict[str, Any]]:
        if not candles:
            return None

        ordered = sorted(list(candles), key=lambda x: int(x.get("close_time") or 0))
        need = self.required_bars_for(ema_slow=int(ema_slow), atr_window=int(atr_window))
        if len(ordered) < need:
            return None

        closes = [float(c["close"]) for c in ordered]
        highs = [float(c["high"]) for c in ordered]
        lows = [float(c["low"]) for c in ordered]

        ema_fast_value = self._ema(closes, int(ema_fast))
        ema_slow_value = self._ema(closes, int(ema_slow))
        atr = self._atr(highs, lows, closes, int(atr_window))
        if ema_fast_value is None or ema_slow_value is None or atr is None:
            return None

        last = ordered[-1]
        close = float(last["close"])
        atr_pct = float(atr) / close if close > 0 else 0.0
        created_at_iso = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

        return {
            "stream_key": str(last.get("stream_key") or "").strip().lower(),
            "source": str(last.get("source") or "").strip().lower(),
            "symbol": str(last.get("symbol") or "").strip().upper(),
            "interval": str(last.get("interval") or "").strip().lower(),
            "ts": int(last.get("close_time")),
            "close": float(close),
            "ema_fast": float(ema_fast_value),
            "ema_slow": float(ema_slow_value),
            "atr_pct": float(atr_pct),
            "created_at_iso": created_at_iso,
        }

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