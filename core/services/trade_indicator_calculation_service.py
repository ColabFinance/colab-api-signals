from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from core.domain.enums.trade_enums import TradeMovingAverageType


class TradeIndicatorCalculationService:
    """
    Pure calculation service used by trade evaluators.

    This service computes:
    - ATR
    - ATR percent
    - moving averages for regime filters
    - dynamic ATR thresholds based on rolling percentiles
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

        This implementation matches the new simulator logic:
        - EWM ATR
        - adjust=False
        - min_periods=max(2, window // 2)
        """
        if not highs or not lows or not closes:
            return [], []

        h = pd.to_numeric(pd.Series(list(highs)), errors="coerce")
        l = pd.to_numeric(pd.Series(list(lows)), errors="coerce")
        c = pd.to_numeric(pd.Series(list(closes)), errors="coerce").ffill()

        prev_c = c.shift(1)
        tr = pd.concat(
            [
                (h - l).abs(),
                (h - prev_c).abs(),
                (l - prev_c).abs(),
            ],
            axis=1,
        ).max(axis=1)

        atr = tr.ewm(
            span=max(1, int(window)),
            adjust=False,
            min_periods=max(2, int(window) // 2),
        ).mean()

        atr = atr.replace([np.inf, -np.inf], np.nan).ffill().fillna(0.0)
        atr_pct = (atr / c.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).ffill().fillna(0.0)

        return atr.astype(float).tolist(), atr_pct.astype(float).tolist()

    def compute_ma(
        self,
        *,
        values: Sequence[float],
        window: Optional[int],
        ma_type: TradeMovingAverageType | str = TradeMovingAverageType.EMA,
    ) -> List[float]:
        """
        Compute EMA or SMA for the whole series.

        When window is None or invalid, the result is a NaN series with the same length.
        """
        base = pd.to_numeric(pd.Series(list(values)), errors="coerce").ffill()

        if window is None:
            return pd.Series(np.nan, index=base.index, dtype=float).tolist()

        w = int(window)
        if w <= 0:
            return pd.Series(np.nan, index=base.index, dtype=float).tolist()

        normalized_type = self._normalize_ma_type(ma_type)

        if normalized_type == TradeMovingAverageType.SMA:
            out = base.rolling(
                window=w,
                min_periods=max(1, w // 2),
            ).mean()
        else:
            out = base.ewm(
                span=w,
                adjust=False,
                min_periods=max(1, w // 2),
            ).mean()

        return out.replace([np.inf, -np.inf], np.nan).tolist()

    def normalize_percentile_input(self, value: Optional[float]) -> Optional[float]:
        """
        Normalize percentile input.

        Accepted formats:
        - 0.2 / 0.6
        - 20 / 60
        """
        if value is None:
            return None

        q = float(value)
        if 1.0 < q <= 100.0:
            q = q / 100.0

        if not (0.0 <= q <= 1.0):
            raise ValueError("Percentile must be between 0 and 1, or between 0 and 100.")

        return q

    def compute_dynamic_thresholds(
        self,
        *,
        values: Sequence[float],
        window: int,
        low_percentile: float,
        high_percentile: float,
        min_periods: Optional[int] = None,
    ) -> Tuple[List[float], List[float]]:
        """
        Compute rolling percentile-based thresholds using shifted historical values.

        The base series is shifted by 1 so the current candle never sees its own
        ATR value when computing the active thresholds.
        """
        s = pd.to_numeric(pd.Series(list(values)), errors="coerce").replace([np.inf, -np.inf], np.nan)

        w = int(window)
        mp = int(min_periods) if min_periods is not None else max(2, w // 2)

        base = s.shift(1)

        low_thr = base.rolling(window=w, min_periods=mp).quantile(float(low_percentile))
        high_thr = base.rolling(window=w, min_periods=mp).quantile(float(high_percentile))

        low_thr = low_thr.replace([np.inf, -np.inf], np.nan)
        high_thr = high_thr.replace([np.inf, -np.inf], np.nan)

        return low_thr.tolist(), high_thr.tolist()

    def required_bars_for_atr(self, *, atr_window: int) -> int:
        """
        Return the minimum bars required to compute ATR safely.
        """
        return max(3, int(atr_window) + 2)

    def required_bars_for_ma(self, *, ma_window: int) -> int:
        """
        Return a safe bar count for MA calculations.
        """
        return max(3, int(ma_window) + 2)

    def required_bars_for_dynamic_threshold(self, *, dynamic_window: int) -> int:
        """
        Return a safe bar count for dynamic rolling percentiles.
        """
        return max(3, int(dynamic_window) + 2)

    def _normalize_ma_type(
        self,
        value: TradeMovingAverageType | str,
    ) -> TradeMovingAverageType:
        """
        Normalize MA type values into the enum.
        """
        if isinstance(value, TradeMovingAverageType):
            return value
        return TradeMovingAverageType(str(value).strip().lower())