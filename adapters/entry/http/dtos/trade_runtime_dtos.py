from __future__ import annotations

from pydantic import BaseModel


class TradeBufferStreamOutDTO(BaseModel):
    """
    Read-only DTO for a Redis trade candle buffer stream.
    """

    redis_key: str
    stream_key: str
    candle_count: int


class TradeBufferCandleOutDTO(BaseModel):
    """
    Read-only DTO for a single candle inside the Redis trade candle buffer.
    """

    stream_key: str
    source: str
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int
    is_closed: bool


class TradeBufferBootstrapOutDTO(BaseModel):
    """
    Read-only DTO for a trade candle buffer bootstrap operation.
    """

    ok: bool
    stream_key: str
    stored_count: int
    limit: int