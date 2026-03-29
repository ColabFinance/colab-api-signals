from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.entry.http.deps import get_db
from adapters.entry.http.dtos.trade_strategy_dtos import (
    TradeSignalOutDTO,
    TradeStrategyCreateDTO,
    TradeStrategyOutDTO,
    TradeStrategyStatusSetDTO,
)
from adapters.entry.http.dtos.trade_strategy_runtime_dtos import (
    TradeStrategyRuntimeSnapshotOutDTO,
)
from adapters.external.database.trade_signal_repository_mongodb import TradeSignalRepositoryMongoDB
from adapters.external.database.trade_strategy_repository_mongodb import TradeStrategyRepositoryMongoDB
from adapters.external.database.trade_strategy_runtime_snapshot_repository_mongodb import (
    TradeStrategyRuntimeSnapshotRepositoryMongoDB,
)
from core.usecases.trade_strategy_use_case import TradeStrategyUseCase


router = APIRouter(prefix="/trade-strategies", tags=["trade-strategies"])


def get_use_case(db: AsyncIOMotorDatabase) -> TradeStrategyUseCase:
    """
    Build the trade strategy use case.
    """
    return TradeStrategyUseCase(
        strategy_repo=TradeStrategyRepositoryMongoDB(db),
        signal_repo=TradeSignalRepositoryMongoDB(db),
        runtime_snapshot_repo=TradeStrategyRuntimeSnapshotRepositoryMongoDB(db),
    )


@router.post("", response_model=dict)
async def create_trade_strategy(
    body: TradeStrategyCreateDTO,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Create a new trade strategy.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.create_strategy(body)
        data = TradeStrategyOutDTO.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create trade strategy: {exc}") from exc


@router.get("", response_model=dict)
async def list_trade_strategies(
    stream_key: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    List trade strategies with optional filters.

    This legacy route remains unchanged so existing screens such as tradeHome
    continue working without any frontend changes.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        items = await uc.list_strategies(
            stream_key=stream_key,
            status=status,
            limit=int(limit),
        )
        data = [TradeStrategyOutDTO.model_validate(item.model_dump()) for item in items]
        return {"ok": True, "message": "ok", "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list trade strategies: {exc}") from exc


@router.get("/public", response_model=dict)
async def list_public_trade_strategies(
    status: Optional[str] = Query(None),
    stream_key: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    execution_account_id: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=5000),
    page: Optional[int] = Query(None, ge=1),
    offset: Optional[int] = Query(None, ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    List trade strategies for public user-facing pages with pagination support.

    When only `limit` is sent, the route behaves like a simple limited list.
    When `page` is sent, page-based pagination is applied.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        result = await uc.list_public_strategies(
            status=status,
            stream_key=stream_key,
            symbol=symbol,
            execution_account_id=execution_account_id,
            search=search,
            limit=int(limit),
            page=page,
            offset=offset,
        )

        data = [TradeStrategyOutDTO.model_validate(item.model_dump()) for item in result["items"]]

        return {
            "ok": True,
            "message": "ok",
            "data": data,
            "pagination": result["pagination"],
            "summary": result["summary"],
            "filter_options": result["filter_options"],
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list public trade strategies: {exc}") from exc


@router.post("/status/set", response_model=dict)
async def set_trade_strategy_status(
    body: TradeStrategyStatusSetDTO,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Update the status of an existing trade strategy.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.set_status(strategy_id=body.strategy_id, status=body.status)
        if ent is None:
            raise HTTPException(status_code=404, detail="Trade strategy not found.")

        data = TradeStrategyOutDTO.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update trade strategy status: {exc}") from exc


@router.get("/signals", response_model=dict)
async def list_trade_signals(
    strategy_id: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=5000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    List generated trade signals.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        items = await uc.list_signals(strategy_id=strategy_id, limit=int(limit))
        data = [TradeSignalOutDTO.model_validate(item.model_dump()) for item in items]
        return {"ok": True, "message": "ok", "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list trade signals: {exc}") from exc


@router.get("/runtime/latest", response_model=dict)
async def get_latest_trade_strategy_runtime_snapshot(
    strategy_id: str = Query(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Fetch the latest runtime snapshot for a trade strategy.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.get_latest_runtime_snapshot(strategy_id=strategy_id)
        data = (
            TradeStrategyRuntimeSnapshotOutDTO.model_validate(ent.model_dump())
            if ent is not None
            else None
        )
        return {"ok": True, "message": "ok", "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch latest runtime snapshot: {exc}") from exc


@router.get("/runtime/history", response_model=dict)
async def list_trade_strategy_runtime_history(
    strategy_id: str = Query(...),
    limit: int = Query(200, ge=1, le=5000),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    List runtime snapshot history for a trade strategy.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        items = await uc.list_runtime_snapshots(
            strategy_id=strategy_id,
            limit=int(limit),
        )
        data = [TradeStrategyRuntimeSnapshotOutDTO.model_validate(item.model_dump()) for item in items]
        return {"ok": True, "message": "ok", "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list runtime snapshots: {exc}") from exc


@router.get("/{strategy_id}", response_model=dict)
async def get_trade_strategy_by_id(
    strategy_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Fetch one trade strategy by identifier.
    """
    try:
        uc = get_use_case(db)
        await uc.ensure_indexes()

        ent = await uc.get_strategy_by_id(strategy_id=strategy_id)
        if ent is None:
            raise HTTPException(status_code=404, detail="Trade strategy not found.")

        data = TradeStrategyOutDTO.model_validate(ent.model_dump())
        return {"ok": True, "message": "ok", "data": data}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch trade strategy: {exc}") from exc