from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException, Query, Request

from adapters.entry.http.dtos.trade_runtime_dtos import (
    TradeBufferBootstrapOutDTO,
    TradeBufferCandleOutDTO,
    TradeBufferStreamOutDTO,
)


router = APIRouter(prefix="/trade-runtime", tags=["trade-runtime"])


@router.get("/buffers", response_model=List[TradeBufferStreamOutDTO])
async def list_trade_candle_buffers(
    request: Request,
    limit: int = Query(500, ge=1, le=5000),
) -> List[TradeBufferStreamOutDTO]:
    """
    List all Redis trade candle buffers currently stored.
    """
    uc = getattr(request.app.state, "trade_candle_buffer_uc", None)
    if uc is None:
        raise HTTPException(status_code=503, detail="Trade candle buffer use case is not available.")

    items = await uc.list_streams(limit=int(limit))
    return [TradeBufferStreamOutDTO.model_validate(item) for item in items]


@router.get("/buffers/{stream_key:path}/candles", response_model=List[TradeBufferCandleOutDTO])
async def list_trade_candle_buffer_candles(
    stream_key: str,
    request: Request,
    limit: int = Query(200, ge=1, le=5000),
) -> List[TradeBufferCandleOutDTO]:
    """
    List candles for a specific Redis trade candle buffer.
    """
    uc = getattr(request.app.state, "trade_candle_buffer_uc", None)
    if uc is None:
        raise HTTPException(status_code=503, detail="Trade candle buffer use case is not available.")

    items = await uc.list_candles(
        stream_key=stream_key,
        limit=int(limit),
    )
    return [TradeBufferCandleOutDTO.model_validate(item) for item in items]


@router.post("/buffers/{stream_key:path}/bootstrap", response_model=TradeBufferBootstrapOutDTO)
async def bootstrap_trade_candle_buffer(
    stream_key: str,
    request: Request,
    limit: int = Query(500, ge=1, le=5000),
) -> TradeBufferBootstrapOutDTO:
    """
    Bootstrap a Redis trade candle buffer from api-market-data for a specific stream.
    """
    uc = getattr(request.app.state, "trade_candle_buffer_uc", None)
    if uc is None:
        raise HTTPException(status_code=503, detail="Trade candle buffer use case is not available.")

    stored_count = await uc.bootstrap_stream(
        stream_key=stream_key,
        limit=int(limit),
    )

    return TradeBufferBootstrapOutDTO(
        ok=True,
        stream_key=stream_key,
        stored_count=stored_count,
        limit=int(limit),
    )