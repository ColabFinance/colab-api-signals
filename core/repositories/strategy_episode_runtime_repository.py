from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from core.domain.entities.strategy_episode_runtime_entity import StrategyEpisodeRuntimeEntity


class StrategyEpisodeRuntimeRepository(ABC):
    @abstractmethod
    async def ensure_indexes(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def upsert(self, runtime: StrategyEpisodeRuntimeEntity) -> StrategyEpisodeRuntimeEntity:
        raise NotImplementedError

    @abstractmethod
    async def list_by_episode(
        self,
        *,
        episode_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[StrategyEpisodeRuntimeEntity | None]:
        raise NotImplementedError

    @abstractmethod
    async def count_by_episode(self, *, episode_id: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def list_by_strategy(
        self,
        *,
        strategy_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> List[StrategyEpisodeRuntimeEntity | None]:
        raise NotImplementedError

    @abstractmethod
    async def count_by_strategy(self, *, strategy_id: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def get_latest_by_strategy(
        self,
        *,
        strategy_id: str,
    ) -> Optional[StrategyEpisodeRuntimeEntity]:
        raise NotImplementedError