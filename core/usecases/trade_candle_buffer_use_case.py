from __future__ import annotations

from typing import Any, Dict, List

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.redis.trade_candle_buffer_repository_redis import TradeCandleBufferRepositoryRedis


class TradeCandleBufferUseCase:
    """
    Application layer for inspecting and bootstrapping the Redis trade candle buffer.
    """

    def __init__(
        self,
        *,
        candle_buffer_repo: TradeCandleBufferRepositoryRedis,
        market_data_client: MarketDataHttpClient,
    ) -> None:
        """
        Initialize the trade candle buffer use case.
        """
        self._buffer_repo = candle_buffer_repo
        self._market_data = market_data_client

    async def list_streams(self, *, limit: int = 500) -> List[Dict[str, Any]]:
        """
        List all Redis trade candle buffers currently stored.
        """
        return await self._buffer_repo.list_streams(limit=limit)

    async def list_candles(self, *, stream_key: str, limit: int = 200) -> List[Dict[str, Any]]:
        """
        List candles from a Redis trade candle buffer.
        """
        return await self._buffer_repo.list_last(
            stream_key=stream_key,
            limit=limit,
        )

    async def bootstrap_stream(self, *, stream_key: str, limit: int = 500) -> int:
        """
        Bootstrap a Redis trade candle buffer from api-market-data for a specific stream.
        """
        normalized_stream_key = str(stream_key).strip().lower()
        source, symbol, interval = self._parse_stream_key(normalized_stream_key)

        candles = await self._market_data.list_candles(
            stream_key=normalized_stream_key,
            limit=int(limit),
        )

        normalized = [
            {
                "stream_key": normalized_stream_key,
                "source": source,
                "symbol": candle.get("symbol", symbol),
                "interval": candle.get("interval", interval),
                "open_time": int(candle["open_time"]),
                "close_time": int(candle["close_time"]),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle.get("volume", 0.0)),
                "trades": int(candle.get("trades", 0)),
                "is_closed": bool(candle.get("is_closed", True)),
            }
            for candle in candles
        ]

        await self._buffer_repo.replace(
            stream_key=normalized_stream_key,
            candles=normalized,
            maxlen=int(limit),
        )
        return len(normalized)

    def _parse_stream_key(self, stream_key: str) -> tuple[str, str, str]:
        """
        Parse source, symbol, and interval from a canonical stream key.
        """
        parts = str(stream_key).strip().lower().split(":")
        source = parts[0] if len(parts) > 0 else ""
        symbol = parts[1].upper() if len(parts) > 1 else ""
        interval = parts[2] if len(parts) > 2 else "1m"
        return source, symbol, interval