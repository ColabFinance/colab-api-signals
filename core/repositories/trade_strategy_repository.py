from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol

from core.domain.entities.trade_strategy_entity import TradeStrategyEntity


class TradeStrategyRepository(Protocol):
    """
    Repository contract for trade strategy persistence.
    """

    async def ensure_indexes(self) -> None:
        """
        Ensure repository indexes exist.
        """
        raise NotImplementedError

    async def create(self, strategy: TradeStrategyEntity) -> TradeStrategyEntity:
        """
        Persist a new trade strategy.
        """
        raise NotImplementedError

    async def update(self, strategy_id: str, data: Dict[str, Any]) -> Optional[TradeStrategyEntity]:
        """
        Update an existing trade strategy and return the stored document.
        """
        raise NotImplementedError

    async def get_by_id(self, strategy_id: str) -> Optional[TradeStrategyEntity]:
        """
        Fetch a trade strategy by its Mongo identifier.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    async def list_public(
        self,
        *,
        status: Optional[str] = None,
        stream_key: Optional[str] = None,
        symbol: Optional[str] = None,
        execution_account_id: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[TradeStrategyEntity]:
        """
        List trade strategies for public user-facing pages with filtering and pagination.
        """
        raise NotImplementedError

    async def count_public(
        self,
        *,
        status: Optional[str] = None,
        stream_key: Optional[str] = None,
        symbol: Optional[str] = None,
        execution_account_id: Optional[str] = None,
        search: Optional[str] = None,
    ) -> int:
        """
        Count trade strategies for public user-facing pages with filtering.
        """
        raise NotImplementedError

    async def get_public_summary(self) -> Dict[str, int]:
        """
        Return summary counters for public user-facing trade strategy pages.
        """
        raise NotImplementedError

    async def list_public_filter_options(self) -> Dict[str, List[str]]:
        """
        Return available filter values for public user-facing trade strategy pages.
        """
        raise NotImplementedError

    async def get_active_by_stream_key(self, stream_key: str) -> List[TradeStrategyEntity]:
        """
        List active trade strategies bound to a stream_key.
        """
        raise NotImplementedError

    async def set_status(self, strategy_id: str, status: str) -> Optional[TradeStrategyEntity]:
        """
        Update strategy status and return the stored document.
        """
        raise NotImplementedError