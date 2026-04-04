from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

from config.settings import settings


class MarketDataHttpClient:
    """
    Reusable HTTP client for api-market-data.
    """

    def __init__(
        self,
        *,
        base_url: str,
        timeout_s: float = 30.0,
        connect_timeout_s: float = 5.0,
    ) -> None:
        """
        Initialize the client.

        Args:
            base_url: Base URL for api-market-data.
            timeout_s: Default total timeout.
            connect_timeout_s: Connection timeout.
        """
        self.base_url = str(base_url).rstrip("/")
        self._default_timeout = httpx.Timeout(timeout_s, connect=connect_timeout_s)
        self._client = httpx.AsyncClient(timeout=self._default_timeout)

    @classmethod
    def from_settings(cls) -> "MarketDataHttpClient":
        """
        Build the client from application settings.
        """
        st = settings
        return cls(base_url=(st.MARKET_DATA_BASE_URL or "").rstrip("/"))

    async def aclose(self) -> None:
        """
        Close the underlying reusable HTTP client.
        """
        await self._client.aclose()

    async def get_token_price_usd(
        self,
        *,
        chain: str,
        token_address: str,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch token USD price from api-market-data.
        """
        return await self._get_json(
            path=f"/api/pricing/tokens/{token_address}/usd",
            params={"chain": (chain or "").strip().lower()},
            access_token=access_token,
            timeout_s=15.0,
        )

    async def list_price_ticks(
        self,
        *,
        stream_key: str,
        ts_from: int,
        ts_to: int,
        limit: int = 5000,
        access_token: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch raw price ticks from api-market-data.
        """
        data = await self._get_json(
            path="/api/market-data/price-ticks",
            params={
                "stream_key": stream_key,
                "ts_from": int(ts_from),
                "ts_to": int(ts_to),
                "limit": int(limit),
            },
            access_token=access_token,
            timeout_s=30.0,
        )
        return data if isinstance(data, list) else []

    async def list_candles(
        self,
        *,
        stream_key: str,
        limit: int = 500,
        access_token: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch closed candles from api-market-data.

        Candles are returned in ascending order by time.
        """
        data = await self._get_json(
            path="/api/market-data/candles",
            params={
                "stream_key": str(stream_key).strip().lower(),
                "limit": int(limit),
            },
            access_token=access_token,
            timeout_s=30.0,
        )
        return data if isinstance(data, list) else []

    async def _get_json(
        self,
        *,
        path: str,
        params: Dict[str, Any],
        access_token: Optional[str],
        timeout_s: float,
    ) -> Any:
        """
        Send one GET request and return parsed JSON content.
        """
        headers: Dict[str, str] = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        response = await self._client.get(
            f"{self.base_url}{path}",
            params=params,
            headers=headers,
            timeout=httpx.Timeout(timeout_s, connect=5.0),
        )
        data = response.json() if response.content else {}

        if response.status_code >= 400:
            if isinstance(data, dict):
                raise RuntimeError(data.get("detail") or data.get("message") or f"market_data_error_{response.status_code}")
            raise RuntimeError(f"market_data_error_{response.status_code}")

        return data