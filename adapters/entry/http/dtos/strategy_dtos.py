from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator
from web3 import Web3

from core.domain.entities.strategy_entity import StrategyParams


ChainKey = Literal["base", "bnb"]


class StrategyParamsUpsertRequest(BaseModel):
    chain: ChainKey
    owner: str
    strategy_id: int = Field(..., ge=1)

    name: str = Field(..., examples=["pancake-weth4"])
    symbol: str = Field(..., examples=["ETHUSDT"])
    indicator_set_id: str = Field(..., description="Use the indicator set cfg_hash")
    stream_key: Optional[str] = None
    status: str = Field("ACTIVE", examples=["ACTIVE", "INACTIVE"])
    is_public: bool = False

    params: StrategyParams = Field(default_factory=StrategyParams)

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
    params: Optional[StrategyParams] = None

    alias: Optional[str] = None
    dex: Optional[str] = None
    is_public: bool = False

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


class StrategyRegisterRequest(BaseModel):
    chain: ChainKey
    owner: str
    strategy_id: int = Field(..., ge=1)

    name: str = Field(..., examples=["pancake-weth4"])
    symbol: str = Field(..., examples=["ETHUSDT"])
    indicator_set_id: str = Field(..., description="Use the indicator set cfg_hash")
    stream_key: Optional[str] = None

    status: str = Field("INACTIVE", examples=["ACTIVE", "INACTIVE"])
    is_public: bool = False

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


class StrategyListQuery(BaseModel):
    chain: ChainKey
    owner: str
    status: Optional[str] = None  # "ACTIVE" | "INACTIVE" | None

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        v = (v or "").strip()
        if not Web3.is_address(v):
            raise ValueError("Invalid owner address")
        return v.lower()

    @field_validator("status")
    @classmethod
    def norm_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = (v or "").strip().upper()
        if not vv:
            return None
        return "ACTIVE" if vv == "ACTIVE" else "INACTIVE"


class StrategyExploreQuery(BaseModel):
    chain: Optional[ChainKey] = None
    status: Optional[str] = None
    limit: int = Field(200, ge=1, le=1000)
    offset: int = Field(0, ge=0)

    @field_validator("status")
    @classmethod
    def norm_status(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        vv = (v or "").strip().upper()
        if not vv:
            return None
        return "ACTIVE" if vv == "ACTIVE" else "INACTIVE"


class StrategyVaultLinkRequest(BaseModel):
    chain: ChainKey
    owner: str
    strategy_id: int = Field(..., ge=1)

    dex: str = Field(..., description="ex: pancake_v3|uniswap_v3|aerodrome")
    alias: str = Field(..., description="vault alias generated by api-lp")

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        v = (v or "").strip()
        if not Web3.is_address(v):
            raise ValueError("Invalid owner address")
        return v.lower()

    @field_validator("dex")
    @classmethod
    def validate_dex(cls, v: str) -> str:
        v = (v or "").strip().lower()
        if not v:
            raise ValueError("dex is required")
        return v

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("alias is required")
        return v


class StrategyStatusSetRequest(BaseModel):
    chain: ChainKey
    owner: str
    strategy_id: int = Field(..., ge=1)
    status: str = Field(..., examples=["ACTIVE", "INACTIVE"])

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        v = (v or "").strip()
        if not Web3.is_address(v):
            raise ValueError("Invalid owner address")
        return v.lower()

    @field_validator("status")
    @classmethod
    def norm_status(cls, v: str) -> str:
        vv = (v or "").strip().upper()
        return "ACTIVE" if vv == "ACTIVE" else "INACTIVE"