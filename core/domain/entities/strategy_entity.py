from typing import Any, Dict, Optional
from pydantic import ConfigDict, Field
from .base_entity import MongoEntity


class StrategyEntity(MongoEntity):
    name: str
    symbol: str
    status: str
    indicator_set_id: str
    stream_key: str
    params: Dict[str, Any] = Field(default_factory=dict)

    alias: Optional[str] = None
    dex: Optional[str] = None
    chain: Optional[str] = None  # "base" | "bnb"
    owner: Optional[str] = None  # lowercase 0x...
    strategy_id: Optional[int] = None  # onchain strategyId (uint)
    adapter: Optional[str] = None
    dex_router: Optional[str] = None
    token0: Optional[str] = None
    token1: Optional[str] = None
    tx_hash: Optional[str] = None  # tx that created/updated this registry data

    model_config = ConfigDict(extra="ignore")
