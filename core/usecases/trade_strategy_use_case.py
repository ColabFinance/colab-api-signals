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
        Ensure indexes required by all repositories exist.
        """
        await self.strategy_repo.ensure_indexes()
        await self.signal_repo.ensure_indexes()
        await self.runtime_snapshot_repo.ensure_indexes()

    async def create_strategy(self, data: TradeStrategyCreateDTO) -> TradeStrategyEntity:
        """
        Create and persist one trade strategy from the request DTO.
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
            params=TradeStrategyParamsEntity(**data.params.model_dump(mode="python")),
        )
        return await self.strategy_repo.create(ent)

    async def get_strategy_by_id(self, *, strategy_id: str) -> Optional[TradeStrategyEntity]:
        """
        Fetch one trade strategy by its identifier.
        """
        return await self.strategy_repo.get_by_id(str(strategy_id))

    async def list_strategies(
        self,
        *,
        stream_key: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 500,
    ) -> List[TradeStrategyEntity]:
        """
        List trade strategies using the legacy non-paginated route.
        """
        normalized_status = TradeStrategyStatus(str(status).strip().upper()) if status else None

        return await self.strategy_repo.list(
            stream_key=(str(stream_key).strip().lower() if stream_key else None),
            status=normalized_status.value if normalized_status else None,
            limit=int(limit),
        )

    async def list_public_strategies(
        self,
        *,
        status: Optional[str] = None,
        stream_key: Optional[str] = None,
        symbol: Optional[str] = None,
        execution_account_id: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 20,
        page: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> dict:
        """
        List trade strategies for public user-facing pages with pagination and filters.

        When `page` is provided, it has priority over `offset`.
        When only `limit` is provided, the query behaves like a simple limited list.
        """
        normalized_status = TradeStrategyStatus(str(status).strip().upper()).value if status else None
        normalized_stream_key = str(stream_key).strip().lower() if stream_key else None
        normalized_symbol = str(symbol).strip().upper() if symbol else None
        normalized_execution_account_id = str(execution_account_id).strip() if execution_account_id else None
        normalized_search = str(search).strip() if search else None

        resolved_limit = int(limit)
        resolved_offset = int(offset or 0)

        if page is not None:
            resolved_page = max(1, int(page))
            resolved_offset = (resolved_page - 1) * resolved_limit
        else:
            resolved_page = (resolved_offset // resolved_limit) + 1

        items = await self.strategy_repo.list_public(
            status=normalized_status,
            stream_key=normalized_stream_key,
            symbol=normalized_symbol,
            execution_account_id=normalized_execution_account_id,
            search=normalized_search,
            limit=resolved_limit,
            offset=resolved_offset,
        )
        total = await self.strategy_repo.count_public(
            status=normalized_status,
            stream_key=normalized_stream_key,
            symbol=normalized_symbol,
            execution_account_id=normalized_execution_account_id,
            search=normalized_search,
        )
        summary = await self.strategy_repo.get_public_summary()
        filter_options = await self.strategy_repo.list_public_filter_options()

        return {
            "items": items,
            "pagination": {
                "limit": resolved_limit,
                "offset": resolved_offset,
                "page": resolved_page,
                "total": int(total),
                "has_next": (resolved_offset + resolved_limit) < int(total),
                "has_prev": resolved_offset > 0,
            },
            "summary": summary,
            "filter_options": filter_options,
        }

    async def set_status(self, *, strategy_id: str, status: str) -> Optional[TradeStrategyEntity]:
        """
        Update the status of one stored trade strategy.
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
        List trade signals for one strategy or for the whole module.
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