from __future__ import annotations

import hashlib
import json


class TradeIdempotencyKeyService:
    """
    Build deterministic idempotency keys for generated trade signals.
    """

    def build(
        self,
        *,
        strategy_id: str,
        stream_key: str,
        ts: int,
        signal_type: str,
    ) -> str:
        """
        Build a stable key for a trade signal.

        The same combination of strategy, stream, candle timestamp and signal type
        always produces the same key.
        """
        raw = json.dumps(
            {
                "strategy_id": str(strategy_id),
                "stream_key": str(stream_key).strip().lower(),
                "ts": int(ts),
                "signal_type": str(signal_type).strip().upper(),
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()