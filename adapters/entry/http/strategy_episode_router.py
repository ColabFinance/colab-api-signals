from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.entry.http.deps import get_db
from adapters.entry.http.auth.user_auth import require_user, UserPrincipal
from adapters.entry.http.dtos.strategy_episode_dtos import EpisodesByVaultQuery, EpisodesByVaultResponse, StrategyEpisodeOut

from adapters.external.database.strategy_episode_repository_mongodb import StrategyEpisodeRepositoryMongoDB


router = APIRouter(prefix="/episodes", tags=["episodes"])


@router.get("/by_vault", response_model=EpisodesByVaultResponse)
async def list_episodes_by_vault(
    dex: str = Query(...),
    alias: str = Query(...),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
    # user: UserPrincipal = Depends(require_user),
):
    """
    Returns strategy episodes linked to a vault (dex+alias).
    Auth: user must be logged in (same pattern as others).
    """
    try:
        q = EpisodesByVaultQuery(dex=dex, alias=alias, status=status, limit=limit, offset=offset)

        repo = StrategyEpisodeRepositoryMongoDB(db)
        await repo.ensure_indexes()

        items = await repo.list_by_vault(dex=q.dex, alias=q.alias, status=q.status, limit=q.limit, offset=q.offset)
        total = await repo.count_by_vault(dex=q.dex, alias=q.alias, status=q.status)

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
