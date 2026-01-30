from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from privy import PrivyAPI

from config.settings import settings

bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class UserPrincipal:
    """
    Authenticated user identity derived from a Privy access token.
    """
    privy_did: str
    wallet_address: str  # lowercased 0x...


@lru_cache(maxsize=1)
def _privy_client() -> PrivyAPI:
    if not getattr(settings, "PRIVY_APP_ID", None):
        raise RuntimeError("Missing settings.PRIVY_APP_ID")
    if not getattr(settings, "PRIVY_APP_SECRET", None):
        raise RuntimeError("Missing settings.PRIVY_APP_SECRET")

    return PrivyAPI(app_id=settings.PRIVY_APP_ID, app_secret=settings.PRIVY_APP_SECRET)


def _extract_wallet_from_privy_user(user: Any) -> str:
    """
    Extracts an Ethereum address from a Privy "user" object/dict.
    """
    def get(obj: Any, key: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    for key in ["wallet_address", "address"]:
        v = get(user, key)
        if isinstance(v, str) and v.startswith("0x"):
            return v

    wallet_obj = get(user, "wallet")
    addr = get(wallet_obj, "address") or get(wallet_obj, "wallet_address")
    if isinstance(addr, str) and addr.startswith("0x"):
        return addr

    wallets = get(user, "wallets")
    if isinstance(wallets, list):
        for w in wallets:
            addr = get(w, "address") or get(w, "wallet_address")
            if isinstance(addr, str) and addr.startswith("0x"):
                return addr

    linked = get(user, "linked_accounts")
    if isinstance(linked, list):
        for acc in linked:
            acc_type = (get(acc, "type") or "").lower()
            addr = get(acc, "address") or get(acc, "wallet_address")
            if acc_type == "wallet" and isinstance(addr, str) and addr.startswith("0x"):
                return addr
            if isinstance(addr, str) and addr.startswith("0x"):
                return addr

    return ""


def _get_user_by_did(client: PrivyAPI, did: str) -> Any:
    users = client.users

    fn = getattr(users, "get", None)
    if callable(fn):
        return fn(did)

    fn = getattr(users, "get_by_id", None)
    if callable(fn):
        return fn(user_id=did)

    fn = getattr(users, "retrieve", None)
    if callable(fn):
        return fn(user_id=did)

    raise RuntimeError("Privy SDK does not expose a method to fetch user by DID (users.get/get_by_id/retrieve).")


def require_user(
    creds: HTTPAuthorizationCredentials = Depends(bearer),
) -> UserPrincipal:
    if not creds or not creds.credentials:
        raise HTTPException(status_code=401, detail="Missing Authorization bearer token.")

    token = creds.credentials

    try:
        client = _privy_client()

        claims = client.users.verify_access_token(auth_token=token)

        privy_did = ""
        if isinstance(claims, dict):
            privy_did = str(claims.get("user_id") or "")
        else:
            privy_did = str(getattr(claims, "user_id", "") or "")

        if not privy_did:
            raise HTTPException(status_code=401, detail="Invalid token (missing user_id).")

        user = _get_user_by_did(client, privy_did)
        wallet = _extract_wallet_from_privy_user(user).lower()

        if not wallet:
            raise HTTPException(status_code=403, detail="Token verified but user has no linked wallet address.")

        return UserPrincipal(privy_did=privy_did, wallet_address=wallet)

    except HTTPException:
        raise
    except Exception as e:
        msg = str(e) or "Invalid token"
        low = msg.lower()
        if "invalid" in low or "expired" in low or "auth token" in low:
            raise HTTPException(status_code=401, detail=msg)
        raise HTTPException(status_code=401, detail=f"Authentication failed: {msg}")
