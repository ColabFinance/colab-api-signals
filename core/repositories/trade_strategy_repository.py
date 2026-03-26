from __future__ import annotations

from typing import List, Optional, Protocol

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity


class TradeStrategyRepository(Protocol):
    """
    Repository contract for trade strategy persistence.
    """

    async def ensure_indexes(self) -> None:
        """
        Ensure repository indexes exist.
        """
        ...

    async def create(self, strategy: TradeStrategyEntity) -> TradeStrategyEntity:
        """
        Persist a new trade strategy.
        """
        ...

    async def get_by_id(self, strategy_id: str) -> Optional[TradeStrategyEntity]:
        """
        Fetch a trade strategy by its Mongo identifier.
        """
        ...

    async def list(
        self,
        *,
        stream_key: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 500,
    ) -> List[TradeStrategyEntity]:
        """
        List trade strategies with optional filters.
        """
        ...

    async def get_active_by_stream_key(self, stream_key: str) -> List[TradeStrategyEntity]:
        """
        List active trade strategies bound to a stream_key.
        """
        ...

    async def set_status(self, strategy_id: str, status: str) -> Optional[TradeStrategyEntity]:
        """
        Update strategy status and return the stored document.
        """
        ...