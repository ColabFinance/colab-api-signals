from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity


class StrategyEpisodeRepository(ABC):
    @abstractmethod
    async def ensure_indexes(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_open_by_strategy(self, strategy_id: str) -> Optional[StrategyEpisodeEntity]:
        raise NotImplementedError

    @abstractmethod
    async def open_new(self, episode: StrategyEpisodeEntity) -> StrategyEpisodeEntity:
        raise NotImplementedError

    @abstractmethod
    async def close_episode(self, episode_id: str, close_fields: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def update_partial(self, episode_id: str, partial: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_by_strategy(self, strategy_id: str, limit: int = 50) -> List[StrategyEpisodeEntity | None]:
        raise NotImplementedError

    @abstractmethod
    async def append_execution_log(self, episode_id: str, log: Dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_by_vault(
        self,
        *,
        dex: str,
        alias: str,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[StrategyEpisodeEntity | None]:
        raise NotImplementedError

    @abstractmethod
    async def count_by_vault(
        self,
        *,
        dex: str,
        alias: str,
        status: Optional[str] = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    async def summarize_by_vault_refs(
        self,
        *,
        refs: List[Dict[str, str]],
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError