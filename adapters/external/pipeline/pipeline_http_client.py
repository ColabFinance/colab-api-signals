import logging
from typing import Any, Dict, Optional

import httpx


class PipelineHttpClient:
    """
    Thin async HTTP wrapper around the vault endpoints exposed by api-liquidity-provider.

    All URLs are:
      {base_url}/api/vaults/{dex}/{alias}/...

    This client does *no* strategy logic, only raw HTTP.
    """

    def __init__(self, base_url: str, timeout_sec: float = 180.0):
        self._base_url = base_url.rstrip("/")
        self._logger = logging.getLogger(self.__class__.__name__)

        timeout = timeout_sec
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)

        self._client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            http2=True,
        )
    
    async def aclose(self) -> None:
        await self._client.aclose()
        
    def _build_headers(self, idempotency_key: Optional[str] = None) -> dict:
        headers: dict = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return headers
    
    async def get_status(self, alias_or_address: str, idempotency_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
        url = f"{self._base_url}/api/vaults/{alias_or_address}/status"
        try:
            r = await self._client.get(url, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("status non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("get_status error for %s: %s", url, exc)
        return None

    async def post_collect(
        self, 
        dex: str, 
        alias: str,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/collect
        body: { "alias": <alias> }
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/collect"
        try:
            r = await self._client.post(url, json={"alias": alias}, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("collect non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_collect error for %s: %s", url, exc)
        return None

    async def post_withdraw(
        self, 
        dex: str, 
        alias: str, 
        mode: str = "pool",
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/withdraw
        body: { "alias": <alias>, "mode": "pool" }
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/withdraw"
        try:
            r = await self._client.post(url, json={"alias": alias, "mode": mode}, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("withdraw non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_withdraw error for %s: %s", url, exc)
        return None

    async def post_swap_exact_in(
        self,
        dex: str,
        alias: str,
        token_in: str,
        token_out: str,
        amount_in: Optional[float] = None,
        amount_in_usd: Optional[float] = None,
        convert_gauge_to_usdc: Optional[bool] = False,
        pool_override: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/swap/exact-in
        body:
        {
          "token_in": "...",
          "token_out": "...",
          "amount_in": float,
          "amount_in_usd": float
        }
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/swap/exact-in"
        payload = {
            "token_in": token_in,
            "token_out": token_out,
            "amount_in": amount_in,
            "amount_in_usd": amount_in_usd,
            "convert_gauge_to_usdc": convert_gauge_to_usdc,
            "pool_override": pool_override,
        }
        try:
            r = await self._client.post(url, json=payload, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("swap non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_swap_exact_in error for %s: %s", url, exc)
        return None

    async def post_rebalance(
        self,
        dex: str,
        alias: str,
        lower_price: float,
        upper_price: float,
        cap0: Optional[float] = None,
        cap1: Optional[float] = None,
        lower_tick: Optional[int] = None,
        upper_tick: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/rebalance
        body:
        {
          "lower_tick": int,
          "upper_tick": int,
          "lower_price": float,
          "upper_price": float,
          "cap0": float,
          "cap1": float
        }

        We send ticks if we know them; otherwise 0 and let the provider compute.
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/rebalance"
        payload = {
            "lower_tick": lower_tick if lower_tick is not None else 0,
            "upper_tick": upper_tick if upper_tick is not None else 0,
            "lower_price": lower_price,
            "upper_price": upper_price,
        }
        try:
            r = await self._client.post(url, json=payload, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("rebalance non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_rebalance error for %s: %s", url, exc)
        return None

    async def post_open(
        self,
        dex: str,
        alias: str,
        lower_price: Optional[float] = None,
        upper_price: Optional[float] = None,
        lower_tick: Optional[int] = None,
        upper_tick: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/open

        body:
        {
          "lower_tick": int | null,
          "upper_tick": int | null,
          "lower_price": float | null,
          "upper_price": float | null
        }

        Sem cap0/cap1 aqui, porque na versão nova o open só precisa saber
        qual faixa abrir. O contrato usa os saldos idle atuais do vault.
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/open"
        payload = {"lower_tick": lower_tick, "upper_tick": upper_tick, "lower_price": lower_price, "upper_price": upper_price}
        try:
            r = await self._client.post(url, json=payload, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("open non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_open error for %s: %s", url, exc)
        return None
    
    async def post_unstake(
        self,
        dex: str,
        alias: str,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/unstake
        body: {}
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/unstake"
        try:
            r = await self._client.post(url, json={}, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("unstake non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_unstake error for %s: %s", url, exc)
        return None

    async def post_stake(
        self,
        dex: str,
        alias: str,
        token_id: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/{dex}/{alias}/stake
        body: { "token_id": int|null }
        """
        url = f"{self._base_url}/api/vaults/{dex}/{alias}/stake"
        payload = {}
        try:
            r = await self._client.post(url, json=payload, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("stake non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_stake error for %s: %s", url, exc)
        return None
    
    async def post_auto_rebalance_pancake(
        self,
        alias: str,
        token_in: str,
        token_out: str,
        swap_amount_in: float = 0,
        swap_amount_out_min: float = 0,
        sqrt_price_limit_x96: int = 0,
        gas_strategy: str = "buffered",
        lower_tick: Optional[int] = None,
        upper_tick: Optional[int] = None,
        lower_price: Optional[float] = None,
        upper_price: Optional[float] = None,
        fee: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        POST /api/vaults/pancake/{alias}/auto-rebalance-pancake

        body:
        {
          "new_lower": int,
          "new_upper": int,
          "fee": int,
          "token_in": "0x...",
          "token_out": "0x...",
          "swap_amount_in": float,
          "swap_amount_out_min": float,
          "sqrt_price_limit_x96": int,
          "gas_strategy": "buffered"
        }
        """
        url = f"{self._base_url}/api/vaults/pancake/{alias}/auto-rebalance-pancake"

        payload = {
            "lower_tick": lower_tick,
            "upper_tick": upper_tick,
            "lower_price": lower_price,
            "upper_price": upper_price,
            "fee": int(fee) if fee else None,
            "token_in": token_in,
            "token_out": token_out,
            "swap_amount_in": swap_amount_in,
            "swap_amount_out_min": swap_amount_out_min,
            "sqrt_price_limit_x96": int(sqrt_price_limit_x96 or 0),
            "gas_strategy": gas_strategy,
        }

        try:
            r = await self._client.post(url, json=payload, headers=self._build_headers(idempotency_key))
            if r.status_code == 200:
                return r.json()
            self._logger.warning("auto-rebalance-pancake non-200 %s: %s %s", url, r.status_code, r.text)
        except Exception as exc:
            self._logger.exception("post_auto_rebalance_pancake error for %s: %s", url, exc)
        return None
