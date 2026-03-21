from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.repositories.strategy_episode_repository import StrategyEpisodeRepository


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

    async def ensure_indexes(self) -> None:
        await self.repo.ensure_indexes()

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