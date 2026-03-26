from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from adapters.entry.http.dtos.trade_strategy_dtos import TradeStrategyCreateDTO
from core.domain.entities.trade_strategy_entity import (
    TradeStrategyEntity,
    TradeStrategyParamsEntity,
)
from core.domain.enums.trade_enums import TradeStrategyStatus
from core.repositories.trade_signal_repository import TradeSignalRepository
from core.repositories.trade_strategy_repository import TradeStrategyRepository
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)


@dataclass
class TradeStrategyUseCase:
    """
    Application use case for managing trade strategies and reading generated trade signals.
    """

    strategy_repo: TradeStrategyRepository
    signal_repo: TradeSignalRepository
    runtime_snapshot_repo: TradeStrategyRuntimeSnapshotRepository

    async def ensure_indexes(self) -> None:
        """
        Ensure indexes required by repositories.
        """
        await self.strategy_repo.ensure_indexes()
        await self.signal_repo.ensure_indexes()
        await self.runtime_snapshot_repo.ensure_indexes()

    async def create_strategy(self, data: TradeStrategyCreateDTO) -> TradeStrategyEntity:
        """
        Create a new trade strategy from the request DTO.
        """
        ent = TradeStrategyEntity(
            name=str(data.name).strip(),
            symbol=str(data.symbol).strip().upper(),
            source=str(data.source).strip().lower(),
            interval=str(data.interval).strip().lower(),
            stream_key=str(data.stream_key).strip().lower(),
            strategy_type=data.strategy_type,
            status=data.status,
            execution_target=data.execution_target,
            execution_account_id=(str(data.execution_account_id).strip() if data.execution_account_id else None),
            params=TradeStrategyParamsEntity(**data.params.model_dump()),
        )
        return await self.strategy_repo.create(ent)

    async def list_strategies(
        self,
        *,
        stream_key: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 500,
    ) -> List[TradeStrategyEntity]:
        """
        List strategies with optional filters.
        """
        normalized_status = TradeStrategyStatus(str(status).strip().upper()) if status else None

        return await self.strategy_repo.list(
            stream_key=(str(stream_key).strip().lower() if stream_key else None),
            status=normalized_status.value if normalized_status else None,
            limit=int(limit),
        )

    async def set_status(self, *, strategy_id: str, status: str) -> Optional[TradeStrategyEntity]:
        """
        Update strategy status.
        """
        normalized_status = TradeStrategyStatus(str(status).strip().upper())
        return await self.strategy_repo.set_status(str(strategy_id), normalized_status.value)

    async def list_signals(
        self,
        *,
        strategy_id: Optional[str] = None,
        limit: int = 200,
    ):
        """
        List generated trade signals.
        """
        return await self.signal_repo.list(strategy_id=strategy_id, limit=int(limit))

    async def get_latest_runtime_snapshot(self, *, strategy_id: str):
        """
        Fetch the latest runtime snapshot for a strategy.
        """
        return await self.runtime_snapshot_repo.get_latest_by_strategy_id(str(strategy_id))

    async def list_runtime_snapshots(
        self,
        *,
        strategy_id: str,
        limit: int = 200,
    ):
        """
        List runtime snapshot history for a strategy.
        """
        return await self.runtime_snapshot_repo.list_by_strategy_id(
            strategy_id=str(strategy_id),
            limit=int(limit),
        )