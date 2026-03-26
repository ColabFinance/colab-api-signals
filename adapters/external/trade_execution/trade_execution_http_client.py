from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


class TradeExecutionHttpClient:
    """
    Reusable HTTP client for api-trade-execution.
    """

    def __init__(self, *, base_url: str, timeout_s: float = 30.0) -> None:
        """
        Initialize the client.

        Args:
            base_url: Base URL for api-trade-execution.
            timeout_s: Total timeout in seconds.
        """
        self._base_url = str(base_url).rstrip("/")
        self._timeout = httpx.Timeout(timeout_s, connect=5.0)
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def aclose(self) -> None:
        """
        Close the underlying reusable HTTP client.
        """
        await self._client.aclose()

    async def open_position(
        self,
        *,
        strategy_id: str,
        execution_account_id: str,
        symbol: str,
        position_side: str,
        signal_id: Optional[str],
        signal_ts: int,
        signal_type: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        """
        Call the open-position endpoint.

        Returns:
            Parsed JSON response.
        """
        payload: Dict[str, Any] = {
            "strategy_id": str(strategy_id),
            "execution_account_id": str(execution_account_id),
            "symbol": str(symbol).upper(),
            "position_side": str(position_side).upper(),
            "signal_id": signal_id,
            "signal_ts": int(signal_ts),
            "signal_type": str(signal_type).upper(),
            "idempotency_key": str(idempotency_key),
        }
        return await self._post_json("/api/trade-execution/open", payload)

    async def close_position(
        self,
        *,
        strategy_id: str,
        execution_account_id: str,
        symbol: str,
        close_reason: str,
        signal_id: Optional[str],
        signal_ts: int,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        """
        Call the close-position endpoint.

        Returns:
            Parsed JSON response.
        """
        payload: Dict[str, Any] = {
            "strategy_id": str(strategy_id),
            "execution_account_id": str(execution_account_id),
            "symbol": str(symbol).upper(),
            "close_reason": str(close_reason).upper(),
            "signal_id": signal_id,
            "signal_ts": int(signal_ts),
            "idempotency_key": str(idempotency_key),
        }
        return await self._post_json("/api/trade-execution/close", payload)

    async def _post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send one POST JSON request.

        Args:
            path: Relative API path.
            payload: JSON payload.

        Returns:
            Parsed JSON response.

        Raises:
            RuntimeError: When the remote API returns an error payload.
        """
        response = await self._client.post(f"{self._base_url}{path}", json=payload)
        data = response.json() if response.content else {}
        if response.status_code >= 400:
            if isinstance(data, dict):
                raise RuntimeError(data.get("detail") or data.get("message") or f"trade_execution_error_{response.status_code}")
            raise RuntimeError(f"trade_execution_error_{response.status_code}")
        return data if isinstance(data, dict) else {}