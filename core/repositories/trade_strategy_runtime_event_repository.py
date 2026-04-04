from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from core.domain.entities.trade_strategy_runtime_event_entity import (
    TradeStrategyRuntimeEventEntity,
)


class TradeStrategyRuntimeEventRepository(ABC):
    """
    Repository contract for trade strategy runtime event history.
    """

    @abstractmethod
    async def ensure_indexes(self) -> None:
        """
        Ensure repository indexes exist.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert(self, event: TradeStrategyRuntimeEventEntity) -> None:
        """
        Persist a runtime event using an idempotent key.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_last_by_strategy_id(
        self,
        strategy_id: str,
        limit: int,
    ) -> List[TradeStrategyRuntimeEventEntity]:
        """
        List the most recent runtime events for a strategy in ascending order.
        """
        raise NotImplementedError