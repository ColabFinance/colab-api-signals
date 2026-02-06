from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from config.settings import settings

import httpx


@dataclass
class MarketDataHttpClient:
    base_url: str

    @classmethod
    def from_settings(cls) -> "MarketDataHttpClient":
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
        Calls api-market-data:
          GET /pricing/tokens/{token_address}/usd?chain=...

        Expected response: TokenPriceOutDTO-like json.
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
