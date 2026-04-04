from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from adapters.entry.http.dtos.trade_strategy_dtos import (
    TradeStrategyCreateDTO,
    TradeStrategyUpdateDTO,
)
from core.domain.entities.trade_strategy_entity import (
    TradeStrategyEntity,
    TradeStrategyParamsEntity,
)
from core.domain.enums.trade_enums import TradeStrategyStatus
from core.repositories.trade_signal_repository import TradeSignalRepository
from core.repositories.trade_strategy_repository import TradeStrategyRepository
from core.repositories.trade_strategy_runtime_event_repository import (
    TradeStrategyRuntimeEventRepository,
)
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)


@dataclass
class TradeStrategyUseCase:
    """
    Application use case for managing trade strategies and reading runtime/signal data.
    """

    strategy_repo: TradeStrategyRepository
    signal_repo: TradeSignalRepository
    runtime_snapshot_repo: TradeStrategyRuntimeSnapshotRepository
    runtime_event_repo: TradeStrategyRuntimeEventRepository

    async def ensure_indexes(self) -> None:
        """
        Ensure indexes required by all repositories exist.
        """
        await self.strategy_repo.ensure_indexes()
        await self.signal_repo.ensure_indexes()
        await self.runtime_snapshot_repo.ensure_indexes()
        await self.runtime_event_repo.ensure_indexes()

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

    async def update_strategy(
        self,
        *,
        strategy_id: str,
        data: TradeStrategyUpdateDTO,
    ) -> Optional[TradeStrategyEntity]:
        """
        Partially update one stored trade strategy.

        The incoming payload is merged with the stored document and then
        fully validated through the canonical entities before persisting.
        """
        current = await self.strategy_repo.get_by_id(str(strategy_id))
        if current is None:
            return None

        current_raw = current.model_dump(mode="python")
        current_params_raw = current.params.model_dump(mode="python")

        payload = data.model_dump(mode="python", exclude_none=True)
        incoming_params_raw = payload.pop("params", None)

        merged_params_raw = dict(current_params_raw)
        if incoming_params_raw:
            merged_params_raw.update(incoming_params_raw)

        validated_params = TradeStrategyParamsEntity(**merged_params_raw)

        merged_raw = {
            "name": current_raw["name"],
            "symbol": current_raw["symbol"],
            "source": current_raw["source"],
            "interval": current_raw["interval"],
            "stream_key": current_raw["stream_key"],
            "strategy_type": current_raw["strategy_type"],
            "status": current_raw["status"],
            "execution_target": current_raw["execution_target"],
            "execution_account_id": current_raw.get("execution_account_id"),
            "params": validated_params,
        }

        merged_raw.update(payload)

        if merged_raw.get("name") is not None:
            merged_raw["name"] = str(merged_raw["name"]).strip()

        if merged_raw.get("symbol") is not None:
            merged_raw["symbol"] = str(merged_raw["symbol"]).strip().upper()

        if merged_raw.get("source") is not None:
            merged_raw["source"] = str(merged_raw["source"]).strip().lower()

        if merged_raw.get("interval") is not None:
            merged_raw["interval"] = str(merged_raw["interval"]).strip().lower()

        if merged_raw.get("stream_key") is not None:
            merged_raw["stream_key"] = str(merged_raw["stream_key"]).strip().lower()

        if merged_raw.get("execution_account_id") is not None:
            merged_raw["execution_account_id"] = str(merged_raw["execution_account_id"]).strip()

        validated_entity = TradeStrategyEntity(
            id=current.id,
            name=merged_raw["name"],
            symbol=merged_raw["symbol"],
            source=merged_raw["source"],
            interval=merged_raw["interval"],
            stream_key=merged_raw["stream_key"],
            strategy_type=merged_raw["strategy_type"],
            status=merged_raw["status"],
            execution_target=merged_raw["execution_target"],
            execution_account_id=merged_raw.get("execution_account_id"),
            params=validated_params,
            created_at=current.created_at,
            created_at_iso=current.created_at_iso,
            updated_at=current.updated_at,
            updated_at_iso=current.updated_at_iso,
        )

        update_payload = {
            "name": validated_entity.name,
            "symbol": validated_entity.symbol,
            "source": validated_entity.source,
            "interval": validated_entity.interval,
            "stream_key": validated_entity.stream_key,
            "strategy_type": validated_entity.strategy_type,
            "status": validated_entity.status,
            "execution_target": validated_entity.execution_target,
            "execution_account_id": validated_entity.execution_account_id,
            "params": validated_entity.params.model_dump(mode="python"),
        }

        return await self.strategy_repo.update(str(strategy_id), update_payload)

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

    async def list_signals_paginated(
        self,
        *,
        strategy_id: Optional[str] = None,
        limit: int = 10,
        page: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> dict:
        """
        List trade signals with pagination support for strategy detail screens.
        """
        resolved_limit = int(limit)
        resolved_offset = int(offset or 0)

        if page is not None:
            resolved_page = max(1, int(page))
            resolved_offset = (resolved_page - 1) * resolved_limit
        else:
            resolved_page = (resolved_offset // resolved_limit) + 1

        items = await self.signal_repo.list_paginated(
            strategy_id=strategy_id,
            limit=resolved_limit,
            offset=resolved_offset,
        )
        total = await self.signal_repo.count(strategy_id=strategy_id)

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
        }

    async def get_latest_runtime_snapshot(self, *, strategy_id: str):
        """
        Fetch the latest runtime state for a strategy.
        """
        return await self.runtime_snapshot_repo.get_latest_by_strategy_id(str(strategy_id))

    async def list_runtime_events(
        self,
        *,
        strategy_id: str,
        limit: int = 200,
    ):
        """
        List runtime events for a strategy.
        """
        return await self.runtime_event_repo.list_last_by_strategy_id(
            strategy_id=str(strategy_id),
            limit=int(limit),
        )