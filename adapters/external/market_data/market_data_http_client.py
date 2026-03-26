from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from config.settings import settings


@dataclass
class MarketDataHttpClient:
    """
    HTTP client for api-market-data.
    """

    base_url: str

    @classmethod
    def from_settings(cls) -> "MarketDataHttpClient":
        """
        Build the client from application settings.
        """
        st = settings
        return cls(base_url=(st.MARKET_DATA_BASE_URL or "").rstrip("/"))

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
        url = f"{self.base_url}/api/pricing/tokens/{token_address}/usd"
        params = {"chain": (chain or "").strip().lower()}

        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        async with httpx.AsyncClient(timeout=15.0) as cli:
            res = await cli.get(url, params=params, headers=headers)
            data = res.json() if res.content else {}
            if res.status_code >= 400:
                raise RuntimeError(data.get("detail") or data.get("message") or f"market_data_error_{res.status_code}")
            return data

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
        url = f"{self.base_url}/api/market-data/price-ticks"
        params = {
            "stream_key": stream_key,
            "ts_from": int(ts_from),
            "ts_to": int(ts_to),
            "limit": int(limit),
        }

        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        async with httpx.AsyncClient(timeout=30.0) as cli:
            res = await cli.get(url, params=params, headers=headers)
            data = res.json() if res.content else []
            if res.status_code >= 400:
                if isinstance(data, dict):
                    raise RuntimeError(data.get("detail") or data.get("message") or f"market_data_error_{res.status_code}")
                raise RuntimeError(f"market_data_error_{res.status_code}")
            if isinstance(data, list):
                return data
            return []

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
        url = f"{self.base_url}/api/market-data/candles"
        params = {
            "stream_key": str(stream_key).strip().lower(),
            "limit": int(limit),
        }

        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        async with httpx.AsyncClient(timeout=30.0) as cli:
            res = await cli.get(url, params=params, headers=headers)
            data = res.json() if res.content else []
            if res.status_code >= 400:
                if isinstance(data, dict):
                    raise RuntimeError(data.get("detail") or data.get("message") or f"market_data_error_{res.status_code}")
                raise RuntimeError(f"market_data_error_{res.status_code}")
            if isinstance(data, list):
                return data
            return []