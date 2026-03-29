from __future__ import annotations

from typing import List, Optional, Protocol

from core.domain.entities.trade_signal_entity import TradeSignalEntity


class TradeSignalRepository(Protocol):
    """
    Repository contract for trade signal persistence.
    """

    async def ensure_indexes(self) -> None:
        """
        Ensure repository indexes exist.
        """
        raise NotImplementedError

    async def upsert_by_idempotency_key(self, signal: TradeSignalEntity) -> TradeSignalEntity:
        """
        Upsert a trade signal using its idempotency key.
        """
        raise NotImplementedError

    async def list(
        self,
        *,
        strategy_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[TradeSignalEntity]:
        """
        List generated trade signals.
        """
        raise NotImplementedError

    async def list_pending(self, limit: int) -> List[TradeSignalEntity]:
        """
        List pending trade signals.
        """
        raise NotImplementedError

    async def mark_success(
        self,
        signal: TradeSignalEntity,
        execution_response: Optional[dict] = None,
    ) -> None:
        """
        Mark a trade signal as successfully executed.
        """
        raise NotImplementedError

    async def mark_failure(
        self,
        signal: TradeSignalEntity,
        error_message: str,
    ) -> None:
        """
        Mark a trade signal as failed.
        """
        raise NotImplementedError