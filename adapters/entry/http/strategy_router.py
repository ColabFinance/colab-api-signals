from __future__ import annotations
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.entry.http.deps import get_db
from adapters.entry.http.dtos.strategy_dtos import StrategyExistsQuery, StrategyExistsResponse, StrategyListQuery, StrategyParamsUpsertRequest, StrategyParamsOut, StrategyRegisterRequest, StrategyStatusSetRequest, StrategyVaultLinkRequest
from adapters.entry.http.auth.user_auth import require_user, UserPrincipal

from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from core.usecases.strategy_use_case import StrategyParamsUseCase


router = APIRouter(prefix="/strategies", tags=["strategies"])


def get_use_case(db: AsyncIOMotorDatabase) -> StrategyParamsUseCase:
    repo = StrategyRepositoryMongoDB(db)
    return StrategyParamsUseCase(repo=repo)


@router.get("/params", response_model=dict)
async def get_strategy_params(
    chain: str = Query(...),
    owner: str = Query(...),
    strategy_id: int = Query(..., ge=1),
    db: AsyncIOMotorDatabase = Depends(get_db),
    user: UserPrincipal = Depends(require_user),
):
    try:
        # Authorization: user can only read their own owner doc
        if (owner or "").strip().lower() != user.wallet_address:
            raise HTTPException(status_code=403, detail="Not authorized for this owner address.")

        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.get_by_onchain_identity(chain=chain, owner=owner, strategy_id=strategy_id)
        data = StrategyParamsOut.model_validate(ent.model_dump()) if ent else None
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get strategy params: {exc}") from exc


@router.post("/params/upsert", response_model=dict)
async def upsert_strategy_params(
    body: StrategyParamsUpsertRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
    user: UserPrincipal = Depends(require_user),
):
    try:
        # Authorization: user can only upsert their own owner doc
        if (body.owner or "").strip().lower() != user.wallet_address:
            raise HTTPException(status_code=403, detail="Not authorized for this owner address.")

        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.upsert_params(data=body)
        data = StrategyParamsOut.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upsert strategy params: {exc}") from exc


@router.post("/register", response_model=dict)
async def register_strategy(
    body: StrategyRegisterRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
    user: UserPrincipal = Depends(require_user),
):
    try:
        if (body.owner or "").strip().lower() != user.wallet_address:
            raise HTTPException(status_code=403, detail="Not authorized for this owner address.")

        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.upsert_registry_metadata(data=body)
        data = StrategyParamsOut.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as e:
        if str(e) == "DUPLICATE_NAME_SYMBOL":
            raise HTTPException(status_code=409, detail="Strategy already exists with same (name, symbol).")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to register strategy: {exc}") from exc
    
@router.get("/exists", response_model=dict)
async def strategy_exists(
    chain: str = Query(...),
    owner: str = Query(...),
    name: str = Query(...),
    symbol: str = Query(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
    user: UserPrincipal = Depends(require_user),
):
    try:
        # Authorization: user can only query their own owner
        if (owner or "").strip().lower() != user.wallet_address:
            raise HTTPException(status_code=403, detail="Not authorized for this owner address.")

        # Validate/normalize with DTO (keeps router consistent with others)
        q = StrategyExistsQuery(chain=chain, owner=owner, name=name, symbol=symbol)

        uc = get_use_case(db)
        await uc.ensure_indexes()

        exists = await uc.exists_by_name_symbol(name=q.name, symbol=q.symbol)
        data = StrategyExistsResponse(exists=exists)
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to check strategy existence: {exc}") from exc


@router.get("/list", response_model=dict)
async def list_strategies(
    chain: str = Query(...),
    owner: str = Query(...),
    status: Optional[str] = Query(None),
    db: AsyncIOMotorDatabase = Depends(get_db),
    user: UserPrincipal = Depends(require_user),
):
    try:
        if (owner or "").strip().lower() != user.wallet_address:
            raise HTTPException(status_code=403, detail="Not authorized for this owner address.")

        q = StrategyListQuery(chain=chain, owner=owner, status=status)

        uc = get_use_case(db)
        await uc.ensure_indexes()

        ents = await uc.list_by_owner_chain(chain=q.chain, owner=q.owner, status=q.status)
        data = [StrategyParamsOut.model_validate(e.model_dump()) for e in (ents or [])]

        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list strategies: {exc}") from exc
    

@router.post("/vault-link", response_model=dict)
async def link_vault_to_strategy(
    body: StrategyVaultLinkRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    INTERNAL endpoint (no user auth) called by api-lp after vault creation.
    Updates strategy with vault link fields: dex + alias.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.update_vault_link(
            chain=body.chain,
            owner=body.owner,
            strategy_id=body.strategy_id,
            dex=body.dex,
            alias=body.alias,
        )
        data = StrategyParamsOut.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except ValueError as exc:
        if str(exc) == "STRATEGY_NOT_FOUND":
            raise HTTPException(status_code=404, detail="Strategy not found for (chain, owner, strategy_id).") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to link vault to strategy: {exc}") from exc


@router.post("/status/set", response_model=dict)
async def set_strategy_status(
    body: StrategyStatusSetRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
    user: UserPrincipal = Depends(require_user),
):
    try:
        if (body.owner or "").strip().lower() != user.wallet_address:
            raise HTTPException(status_code=403, detail="Not authorized for this owner address.")

        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.set_status(
            chain=body.chain,
            owner=body.owner,
            strategy_id=body.strategy_id,
            status=body.status,
        )
        data = StrategyParamsOut.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        if str(exc) == "STRATEGY_NOT_FOUND":
            raise HTTPException(status_code=404, detail="Strategy not found for (chain, owner, strategy_id).") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to set strategy status: {exc}") from exc
