from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.entry.http.deps import get_db
from adapters.entry.http.dtos.strategy_params_dtos import StrategyParamsUpsertRequest, StrategyParamsOut

from adapters.external.database.strategy_repository_mongodb import StrategyRepositoryMongoDB
from core.usecases.strategy_params_use_case import StrategyParamsUseCase


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
):
    try:
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
):
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.upsert_params(data=body.model_dump())
        data = StrategyParamsOut.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upsert strategy params: {exc}") from exc
