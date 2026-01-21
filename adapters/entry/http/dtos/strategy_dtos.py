from __future__ import annotations

from typing import Any, Dict, Literal, Optional
from pydantic import BaseModel, Field, field_validator
from web3 import Web3


ChainKey = Literal["base", "bnb"]


class StrategyParamsUpsertRequest(BaseModel):
    chain: ChainKey
    owner: str
    strategy_id: int = Field(..., ge=1)

    name: str = Field(..., examples=["pancake-weth4"])
    symbol: str = Field(..., examples=["ETHUSDT"])
    indicator_set_id: str = Field(..., description="Use the indicator set cfg_hash")
    status: str = Field("ACTIVE", examples=["ACTIVE", "INACTIVE"])
    params: Dict[str, Any] = Field(default_factory=dict)

    adapter: Optional[str] = None
    dex_router: Optional[str] = None
    token0: Optional[str] = None
    token1: Optional[str] = None
    tx_hash: Optional[str] = None

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return (v or "").upper().strip()

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        v = (v or "").strip()
        if not Web3.is_address(v):
            raise ValueError("Invalid owner address")
        return v.lower()

    @field_validator("adapter", "dex_router", "token0", "token1")
    @classmethod
    def validate_optional_address(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = v.strip()
        if not vv:
            return None
        if not Web3.is_address(vv):
            raise ValueError("Invalid address")
        return vv

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v


class StrategyParamsOut(BaseModel):
    id: Optional[str] = None

    name: str
    symbol: str
    status: str
    indicator_set_id: str
    params: Optional[Dict[str, Any]] = None

    chain: Optional[str] = None
    owner: Optional[str] = None
    strategy_id: Optional[int] = None
    adapter: Optional[str] = None
    dex_router: Optional[str] = None
    token0: Optional[str] = None
    token1: Optional[str] = None
    tx_hash: Optional[str] = None

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None


from typing import Literal

class StrategyRegisterRequest(BaseModel):
    chain: ChainKey
    owner: str
    strategy_id: int = Field(..., ge=1)

    name: str = Field(..., examples=["pancake-weth4"])

    symbol: str = Field(..., examples=["ETHUSDT"])
    indicator_set_id: str = Field(..., description="Use the indicator set cfg_hash")
    
    status: str = Field("INACTIVE", examples=["ACTIVE", "INACTIVE"])
    adapter: Optional[str] = None
    dex_router: Optional[str] = None
    token0: Optional[str] = None
    token1: Optional[str] = None
    tx_hash: Optional[str] = None

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        v = (v or "").strip().upper()
        if not v:
            raise ValueError("symbol is required")
        return v

    @field_validator("indicator_set_id")
    @classmethod
    def validate_indicator_set_id(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("indicator_set_id is required")
        return v
    
    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        v = (v or "").strip()
        if not Web3.is_address(v):
            raise ValueError("Invalid owner address")
        return v.lower()

    @field_validator("adapter", "dex_router", "token0", "token1")
    @classmethod
    def validate_optional_address(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = v.strip()
        if not vv:
            return None
        if not Web3.is_address(vv):
            raise ValueError("Invalid address")
        return vv

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v

    @field_validator("status")
    @classmethod
    def norm_status(cls, v: str) -> str:
        vv = (v or "").strip().upper()
        return "ACTIVE" if vv == "ACTIVE" else "INACTIVE"


class StrategyExistsResponse(BaseModel):
    exists: bool = Field(..., examples=[True, False])

class StrategyExistsQuery(BaseModel):
    chain: ChainKey
    owner: str
    name: str
    symbol: str

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return (v or "").upper().strip()

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        v = (v or "").strip()
        if not Web3.is_address(v):
            raise ValueError("Invalid owner address")
        return v.lower()

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v