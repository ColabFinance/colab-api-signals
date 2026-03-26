from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np


class TradeIndicatorCalculationService:
    """
    Pure calculation service used by trade evaluators.

    This service computes ATR and ATR percent from OHLC candles and is intentionally
    separated from api-market-data indicator snapshots.
    """

    def compute_atr_and_atr_pct(
        self,
        *,
        highs: Sequence[float],
        lows: Sequence[float],
        closes: Sequence[float],
        window: int,
    ) -> Tuple[List[float], List[float]]:
        """
        Compute ATR and ATR percent for the whole series.

        Args:
            highs: High prices in chronological order.
            lows: Low prices in chronological order.
            closes: Close prices in chronological order.
            window: ATR smoothing window.

        Returns:
            A tuple of two lists:
            - ATR values
            - ATR percentage values
        """
        if not highs or not lows or not closes:
            return [], []

        high_arr = np.array([float(x) for x in highs], dtype=float)
        low_arr = np.array([float(x) for x in lows], dtype=float)
        close_arr = np.array([float(x) for x in closes], dtype=float)

        prev_close = np.roll(close_arr, 1)
        prev_close[0] = close_arr[0]

        tr = np.maximum.reduce(
            [
                np.abs(high_arr - low_arr),
                np.abs(high_arr - prev_close),
                np.abs(low_arr - prev_close),
            ]
        )

        atr = self._ewm_mean(values=tr.tolist(), span=max(1, int(window)))
        atr_pct: List[float] = []
        for atr_v, close_v in zip(atr, close_arr.tolist()):
            if float(close_v) <= 0:
                atr_pct.append(0.0)
            else:
                atr_pct.append(float(atr_v) / float(close_v))

        return atr, atr_pct

    def required_bars_for_atr(self, *, atr_window: int) -> int:
        """
        Return the minimum bars required to compute ATR safely.
        """
        return max(3, int(atr_window) + 2)

    def _ewm_mean(self, *, values: Sequence[float], span: int) -> List[float]:
        """
        Compute an exponentially weighted moving average for the full sequence.
        """
        if not values:
            return []

        k = 2.0 / (float(span) + 1.0)
        out: List[float] = []
        ema: Optional[float] = None

        for value in values:
            v = float(value)
            if ema is None:
                ema = v
            else:
                ema = (v - ema) * k + ema
            out.append(float(ema))

        return out