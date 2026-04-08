from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.entry.http.deps import get_db
from adapters.entry.http.dtos.strategy_episode_dtos import (
    EpisodeRuntimeByEpisodeQuery,
    EpisodeRuntimeByEpisodeResponse,
    EpisodeRuntimeByStrategyQuery,
    EpisodeRuntimeByStrategyResponse,
    EpisodeRuntimeLatestResponse,
    EpisodesByVaultQuery,
    EpisodesByVaultResponse,
    EpisodesSummaryByVaultsRequest,
    EpisodesSummaryByVaultsResponse,
    StrategyEpisodeOut,
    StrategyEpisodeRuntimeOut,
    StrategyEpisodeVaultSummaryOut,
)
from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB
from adapters.external.database.strategy_episode_runtime_repository_mongodb import (
    StrategyEpisodeRuntimeRepositoryMongoDB,
)
from core.usecases.strategy_episode_use_case import StrategyEpisodeUseCase


router = APIRouter(prefix="/episodes", tags=["episodes"])


def get_use_case(db: AsyncIOMotorDatabase) -> StrategyEpisodeUseCase:
    episode_repo = StrategyEpisodeRepositoryMongoDB(db)
    runtime_repo = StrategyEpisodeRuntimeRepositoryMongoDB(db)
    return StrategyEpisodeUseCase(repo=episode_repo, runtime_repo=runtime_repo)


@router.get("/by_vault", response_model=EpisodesByVaultResponse)
async def list_episodes_by_vault(
    dex: str = Query(...),
    alias: str = Query(...),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        q = EpisodesByVaultQuery(dex=dex, alias=alias, status=status, limit=limit, offset=offset)

        uc = get_use_case(db)
        await uc.ensure_indexes()

        items, total = await uc.list_by_vault(
            dex=q.dex,
            alias=q.alias,
            status=q.status,
            limit=q.limit,
            offset=q.offset,
        )

        data = []
        for it in items or []:
            md = it.model_dump(mode="python")
            data.append(StrategyEpisodeOut.model_validate(md))

        return EpisodesByVaultResponse(ok=True, message="ok", data=data, total=int(total))
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list episodes: {exc}") from exc


@router.get("/runtime/by_episode", response_model=EpisodeRuntimeByEpisodeResponse)
async def list_episode_runtime_by_episode(
    episode_id: str = Query(...),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        q = EpisodeRuntimeByEpisodeQuery(
            episode_id=episode_id,
            limit=limit,
            offset=offset,
        )

        uc = get_use_case(db)
        await uc.ensure_indexes()

        items, total = await uc.list_runtime_by_episode(
            episode_id=q.episode_id,
            limit=q.limit,
            offset=q.offset,
        )

        data = []
        for it in items or []:
            if hasattr(it, "model_dump"):
                data.append(StrategyEpisodeRuntimeOut.model_validate(it.model_dump(mode="python")))
            else:
                data.append(StrategyEpisodeRuntimeOut.model_validate(it))

        return EpisodeRuntimeByEpisodeResponse(
            ok=True,
            message="ok",
            data=data,
            total=int(total),
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list episode runtime by episode: {exc}") from exc


@router.get("/runtime/by_strategy", response_model=EpisodeRuntimeByStrategyResponse)
async def list_episode_runtime_by_strategy(
    strategy_id: str = Query(...),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        q = EpisodeRuntimeByStrategyQuery(
            strategy_id=strategy_id,
            limit=limit,
            offset=offset,
        )

        uc = get_use_case(db)
        await uc.ensure_indexes()

        items, total = await uc.list_runtime_by_strategy(
            strategy_id=q.strategy_id,
            limit=q.limit,
            offset=q.offset,
        )

        data = []
        for it in items or []:
            if hasattr(it, "model_dump"):
                data.append(StrategyEpisodeRuntimeOut.model_validate(it.model_dump(mode="python")))
            else:
                data.append(StrategyEpisodeRuntimeOut.model_validate(it))

        return EpisodeRuntimeByStrategyResponse(
            ok=True,
            message="ok",
            data=data,
            total=int(total),
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list episode runtime by strategy: {exc}") from exc


@router.get("/runtime/latest", response_model=EpisodeRuntimeLatestResponse)
async def get_latest_episode_runtime_by_strategy(
    strategy_id: str = Query(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        q = EpisodeRuntimeByStrategyQuery(
            strategy_id=strategy_id,
            limit=1,
            offset=0,
        )

        uc = get_use_case(db)
        await uc.ensure_indexes()

        item = await uc.get_latest_runtime_by_strategy(strategy_id=q.strategy_id)
        if item is None:
            return EpisodeRuntimeLatestResponse(ok=True, message="ok", data=None)

        if hasattr(item, "model_dump"):
            data = StrategyEpisodeRuntimeOut.model_validate(item.model_dump(mode="python"))
        else:
            data = StrategyEpisodeRuntimeOut.model_validate(item)

        return EpisodeRuntimeLatestResponse(ok=True, message="ok", data=data)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to get latest runtime: {exc}") from exc


@router.post("/summary/by_vaults", response_model=EpisodesSummaryByVaultsResponse)
async def summarize_episodes_by_vaults(
    body: EpisodesSummaryByVaultsRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        rows = await uc.summarize_by_vault_refs(
            refs=[item.model_dump() for item in body.items],
        )

        data = [StrategyEpisodeVaultSummaryOut.model_validate(row) for row in rows]
        return EpisodesSummaryByVaultsResponse(ok=True, message="ok", data=data, total=len(data))
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to summarize episodes: {exc}") from exc