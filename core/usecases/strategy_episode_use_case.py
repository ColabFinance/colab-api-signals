from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.repositories.strategy_episode_repository import StrategyEpisodeRepository
from core.repositories.strategy_episode_runtime_repository import StrategyEpisodeRuntimeRepository


def _norm(s: Optional[str]) -> str:
    return (s or "").strip()


def _lower(s: Optional[str]) -> str:
    return _norm(s).lower()


def _upper_status(s: Optional[str]) -> Optional[str]:
    value = _norm(s).upper()
    if not value:
        return None
    if value not in ("OPEN", "CLOSED"):
        return None
    return value


@dataclass
class StrategyEpisodeUseCase:
    repo: StrategyEpisodeRepository
    runtime_repo: StrategyEpisodeRuntimeRepository

    async def ensure_indexes(self) -> None:
        await self.repo.ensure_indexes()
        await self.runtime_repo.ensure_indexes()

    async def list_by_vault(
        self,
        *,
        dex: str,
        alias: str,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Any], int]:
        dex_n = _lower(dex)
        alias_n = _norm(alias)
        status_n = _upper_status(status)

        if not dex_n:
            raise ValueError("dex is required")
        if not alias_n:
            raise ValueError("alias is required")

        items = await self.repo.list_by_vault(
            dex=dex_n,
            alias=alias_n,
            status=status_n,
            limit=int(limit or 50),
            offset=int(offset or 0),
        )
        total = await self.repo.count_by_vault(
            dex=dex_n,
            alias=alias_n,
            status=status_n,
        )
        return items, int(total)

    async def summarize_by_vault_refs(
        self,
        *,
        refs: List[Dict[str, str]],
    ) -> List[Dict[str, Any]]:
        clean_refs: List[Dict[str, str]] = []
        seen = set()

        for ref in refs or []:
            dex_n = _lower(ref.get("dex"))
            alias_n = _norm(ref.get("alias"))

            if not dex_n or not alias_n:
                continue

            key = (dex_n, alias_n)
            if key in seen:
                continue

            seen.add(key)
            clean_refs.append({"dex": dex_n, "alias": alias_n})

        if not clean_refs:
            return []

        return await self.repo.summarize_by_vault_refs(refs=clean_refs)

    async def list_runtime_by_episode(
        self,
        *,
        episode_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> Tuple[List[Any], int]:
        episode_id_n = _norm(episode_id)
        if not episode_id_n:
            raise ValueError("episode_id is required")

        items = await self.runtime_repo.list_by_episode(
            episode_id=episode_id_n,
            limit=int(limit or 200),
            offset=int(offset or 0),
        )
        total = await self.runtime_repo.count_by_episode(
            episode_id=episode_id_n,
        )
        return items, int(total)

    async def list_runtime_by_strategy(
        self,
        *,
        strategy_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> Tuple[List[Any], int]:
        strategy_id_n = _norm(strategy_id)
        if not strategy_id_n:
            raise ValueError("strategy_id is required")

        items = await self.runtime_repo.list_by_strategy(
            strategy_id=strategy_id_n,
            limit=int(limit or 200),
            offset=int(offset or 0),
        )
        total = await self.runtime_repo.count_by_strategy(
            strategy_id=strategy_id_n,
        )
        return items, int(total)

    async def get_latest_runtime_by_strategy(
        self,
        *,
        strategy_id: str,
    ) -> Optional[Any]:
        strategy_id_n = _norm(strategy_id)
        if not strategy_id_n:
            raise ValueError("strategy_id is required")

        return await self.runtime_repo.get_latest_by_strategy(
            strategy_id=strategy_id_n,
        )