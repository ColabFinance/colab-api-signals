from __future__ import annotations

from typing import Protocol


class TradeTriggerEventRepository(Protocol):
    """
    Repository contract for trade trigger idempotency markers.
    """

    async def ensure_indexes(self) -> None:
        """
        Ensure repository indexes exist.
        """
        ...

    async def mark_if_new(self, stream_key: str, ts: int) -> bool:
        """
        Mark a trigger event as new if it was not seen before.
        """
        ...