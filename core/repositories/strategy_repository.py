from abc import ABC, abstractmethod
from typing import List, Optional

from core.domain.entities.strategy_entity import StrategyEntity


class StrategyRepository(ABC):
    """
    Repository interface for strategies that reference an indicator set.
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def upsert(self, strategy: StrategyEntity) -> StrategyEntity:
        """
        Upsert by (name, symbol).
        Keeps backward compatibility with existing docs.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_by_onchain_identity(self, strategy: StrategyEntity) -> StrategyEntity:
        """
        Upsert by (chain, owner, strategy_id) when present.
        This is the canonical key for onchain-linked strategies.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_active_by_indicator_set(self, indicator_set_id: str) -> List[StrategyEntity]:
        raise NotImplementedError

    @abstractmethod
    async def get_by_onchain_identity(self, chain: str, owner: str, strategy_id: int) -> Optional[StrategyEntity]:
        raise NotImplementedError

    @abstractmethod
    async def exists_by_name_symbol(self, name: str, symbol: str) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    async def list_by_owner_chain(
        self,
        chain: str,
        owner: str,
        status: Optional[str] = None,
    ) -> List[StrategyEntity]:
        """
        List strategy docs by (chain, owner), optionally filtering by status.
        """
        raise NotImplementedError
    
    @abstractmethod
    async def list_public(
        self,
        *,
        chain: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[StrategyEntity]:
        raise NotImplementedError
    
    @abstractmethod
    async def get_active_by_stream_key(self, stream_key: str) -> List[StrategyEntity]:
        raise NotImplementedError