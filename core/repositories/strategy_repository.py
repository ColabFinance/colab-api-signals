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
