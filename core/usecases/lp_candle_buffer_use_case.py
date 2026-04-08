from __future__ import annotations

from typing import Any, Dict, List, Optional

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.redis.lp_candle_buffer_repository_redis import LpCandleBufferRepositoryRedis


class LpCandleBufferUseCase:
    """
    Maintains the local LP candle buffer and backfills from api-market-data when needed.
    """

    def __init__(
        self,
        *,
        candle_buffer_repo: LpCandleBufferRepositoryRedis,
        market_data_client: MarketDataHttpClient,
        maxlen: int,
    ) -> None:
        self._repo = candle_buffer_repo
        self._market_data = market_data_client
        self._maxlen = int(maxlen)

    async def append_if_present(
        self,
        *,
        stream_key: str,
        candle: Optional[Dict[str, Any]],
    ) -> None:
        if candle is None:
            return
        await self._repo.append_candle(
            stream_key=str(stream_key).strip().lower(),
            candle=candle,
            maxlen=self._maxlen,
        )

    async def ensure_history(
        self,
        *,
        stream_key: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        normalized_stream_key = str(stream_key).strip().lower()
        current = await self._repo.list_candles(stream_key=normalized_stream_key, limit=int(limit))
        if len(current) >= int(limit):
            return current[-int(limit):]

        fetched = await self._market_data.list_candles(
            stream_key=normalized_stream_key,
            limit=int(max(limit, self._maxlen)),
        )
        if fetched:
            await self._repo.replace_candles(
                stream_key=normalized_stream_key,
                candles=fetched,
                maxlen=self._maxlen,
            )
            return fetched[-int(limit):]

        return current[-int(limit):]