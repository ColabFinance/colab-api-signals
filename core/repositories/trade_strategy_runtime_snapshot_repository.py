from __future__ import annotations

from typing import List, Optional, Protocol

from core.domain.entities.trade_strategy_runtime_snapshot_entity import TradeStrategyRuntimeSnapshotEntity


class TradeStrategyRuntimeSnapshotRepository(Protocol):
    """
    Repository contract for trade strategy runtime snapshots.
    """

    async def ensure_indexes(self) -> None:
        """
        Ensure repository indexes exist.
        """
        raise NotImplementedError

    async def upsert(self, snapshot: TradeStrategyRuntimeSnapshotEntity) -> None:
        """
        Upsert a runtime snapshot using strategy_id and ts.
        """
        raise NotImplementedError

    async def get_latest_by_strategy_id(self, strategy_id: str) -> Optional[TradeStrategyRuntimeSnapshotEntity]:
        """
        Fetch the latest runtime snapshot for a strategy.
        """
        raise NotImplementedError
