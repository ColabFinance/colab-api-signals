from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from adapters.entry.http.dtos.strategy_dtos import StrategyParamsUpsertRequest, StrategyRegisterRequest
from core.domain.entities.strategy_entity import StrategyEntity
from core.repositories.strategy_repository import StrategyRepository


def _norm(s: Optional[str]) -> str:
    return (s or "").strip()


def _lower(s: Optional[str]) -> str:
    return _norm(s).lower()


@dataclass
class StrategyParamsUseCase:
    repo: StrategyRepository

    async def ensure_indexes(self) -> None:
        await self.repo.ensure_indexes()

    async def get_by_onchain_identity(self, *, chain: str, owner: str, strategy_id: int) -> Optional[StrategyEntity]:
        return await self.repo.get_by_onchain_identity(chain=_lower(chain), owner=_lower(owner), strategy_id=int(strategy_id))

    async def upsert_params(self, *, data: StrategyParamsUpsertRequest) -> StrategyEntity:
        """
        Upserts a strategy doc using onchain identity:
          (chain, owner, strategy_id)

        Keeps old fields and params untouched/compatible.
        """
        chain = _lower(data.chain)
        owner = _lower(data.owner)
        strategy_id = int(data.strategy_id or 0)

        if not chain:
            raise ValueError("chain is required")
        if not owner:
            raise ValueError("owner is required")
        if strategy_id <= 0:
            raise ValueError("strategy_id must be >= 1")

        name = _norm(data.name)
        symbol = _norm(data.symbol).upper()
        indicator_set_id = _norm(data.indicator_set_id)
        stream_key = _norm(data.stream_key)
        status = _norm(data.status) or "ACTIVE"
        params = data.params or {}

        if not name:
            raise ValueError("name is required")
        if not symbol:
            raise ValueError("symbol is required")
        if not indicator_set_id:
            raise ValueError("indicator_set_id is required")
        if not isinstance(params, dict):
            raise ValueError("params must be an object")

        ent = StrategyEntity(
            name=name,
            symbol=symbol,
            status=status,
            indicator_set_id=indicator_set_id,
            stream_key=stream_key,
            params=params,

            chain=chain,
            owner=owner,
            strategy_id=strategy_id,
            adapter=_norm(data.adapter) or None,
            dex_router=_norm(data.dex_router) or None,
            token0=_norm(data.token0) or None,
            token1=_norm(data.token1) or None,
            tx_hash=_norm(data.tx_hash) or None,
        )

        return await self.repo.upsert_by_onchain_identity(ent)

    async def upsert_registry_metadata(self, *, data: StrategyRegisterRequest) -> StrategyEntity:
        """
        Upserts registry metadata AFTER onchain strategy registration.

        Identity: (chain, owner, strategy_id)
        """
        chain = _lower(data.chain)
        owner = _lower(data.owner)
        strategy_id = int(data.strategy_id or 0)

        if not chain:
            raise ValueError("chain is required")
        if not owner:
            raise ValueError("owner is required")
        if strategy_id <= 0:
            raise ValueError("strategy_id must be >= 1")

        name = _norm(data.name)
        symbol = _norm(data.symbol).upper()
        indicator_set_id = _norm(data.indicator_set_id)
        stream_key = _norm(data.stream_key)
        status = (_norm(data.status) or "INACTIVE").upper()
        status = "ACTIVE" if status == "ACTIVE" else "INACTIVE"

        if not name:
            raise ValueError("name is required")
        if not symbol:
            raise ValueError("symbol is required")
        if not indicator_set_id:
            raise ValueError("indicator_set_id is required")

        ent = StrategyEntity(
            name=name,
            symbol=symbol,
            indicator_set_id=indicator_set_id,
            stream_key=stream_key,
            status=status,
            params={},  # init

            chain=chain,
            owner=owner,
            strategy_id=strategy_id,
            adapter=_norm(data.adapter) or None,
            dex_router=_norm(data.dex_router) or None,
            token0=_norm(data.token0) or None,
            token1=_norm(data.token1) or None,
            tx_hash=_norm(data.tx_hash) or None,
        )

        return await self.repo.upsert_by_onchain_identity(ent)

    async def exists_by_name_symbol(self, *, name: str, symbol: str) -> bool:
        name = _norm(name)
        symbol = _norm(symbol).upper()
        if not name:
            raise ValueError("name is required")
        if not symbol:
            raise ValueError("symbol is required")
        return await self.repo.exists_by_name_symbol(name, symbol)
    
    async def list_by_owner_chain(self, *, chain: str, owner: str, status: Optional[str] = None) -> List[StrategyEntity]:
        chain = _lower(chain)
        owner = _lower(owner)

        st = _norm(status)
        if st:
            st = st.upper()
            st = "ACTIVE" if st == "ACTIVE" else "INACTIVE"
        else:
            st = None

        if not chain:
            raise ValueError("chain is required")
        if not owner:
            raise ValueError("owner is required")

        return await self.repo.list_by_owner_chain(chain=chain, owner=owner, status=st)

    async def update_vault_link(self, *, chain: str, owner: str, strategy_id: int, dex: str, alias: str) -> StrategyEntity:
        """
        Updates ONLY the vault link fields (dex, alias) for an existing strategy doc.

        Identity: (chain, owner, strategy_id)

        This is called by api-lp after creating/registering a vault.
        """
        chain_n = _lower(chain)
        owner_n = _lower(owner)
        sid = int(strategy_id)

        if not chain_n:
            raise ValueError("chain is required")
        if not owner_n:
            raise ValueError("owner is required")
        if sid <= 0:
            raise ValueError("strategy_id must be >= 1")

        dex_n = _lower(dex)
        alias_n = _lower(_norm(alias))
        if not dex_n:
            raise ValueError("dex is required")
        if not alias_n:
            raise ValueError("alias is required")

        existing = await self.repo.get_by_onchain_identity(chain=chain_n, owner=owner_n, strategy_id=sid)
        if not existing:
            raise ValueError("STRATEGY_NOT_FOUND")

        # preserve everything, only set dex/alias
        data = existing.model_dump()
        data["dex"] = dex_n
        data["alias"] = alias_n

        ent = StrategyEntity(**data)
        return await self.repo.upsert_by_onchain_identity(ent)
    
    async def set_status(self, *, chain: str, owner: str, strategy_id: int, status: str) -> StrategyEntity:
        chain_n = _lower(chain)
        owner_n = _lower(owner)
        sid = int(strategy_id)

        if not chain_n:
            raise ValueError("chain is required")
        if not owner_n:
            raise ValueError("owner is required")
        if sid <= 0:
            raise ValueError("strategy_id must be >= 1")

        st = (_norm(status) or "").upper()
        st = "ACTIVE" if st == "ACTIVE" else "INACTIVE"

        existing = await self.repo.get_by_onchain_identity(chain=chain_n, owner=owner_n, strategy_id=sid)
        if not existing:
            raise ValueError("STRATEGY_NOT_FOUND")

        data = existing.model_dump()
        data["status"] = st

        ent = StrategyEntity(**data)
        return await self.repo.upsert_by_onchain_identity(ent)
    