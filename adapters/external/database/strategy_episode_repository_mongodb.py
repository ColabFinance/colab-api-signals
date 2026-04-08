import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity
from core.domain.entities.strategy_episode_runtime_entity import StrategyEpisodeRuntimeEntity
from core.repositories.strategy_episode_repository import StrategyEpisodeRepository


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _norm_status(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    v = (s or "").strip().upper()
    if v not in ("OPEN", "CLOSED"):
        return None
    return v


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if not math.isfinite(out):
            return None
        return out
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _get_path(data: Any, path: List[str]) -> Any:
    cur = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_numeric(data: Any, paths: List[List[str]]) -> Optional[float]:
    for path in paths:
        found = _safe_float(_get_path(data, path))
        if found is not None:
            return found
    return None


def _recursive_sum_fee_fields(data: Any) -> float:
    if data is None:
        return 0.0

    fee_keys = {
        "fee_usd",
        "fees_usd",
        "collected_fee_usd",
        "collected_fees_usd",
        "total_fee_usd",
        "total_fees_usd",
        "realized_fee_usd",
        "realized_fees_usd",
        "gross_fee_usd",
        "gross_fees_usd",
        "net_fee_usd",
        "net_fees_usd",
    }

    total = 0.0

    if isinstance(data, dict):
        for key, value in data.items():
            if key in fee_keys:
                num = _safe_float(value)
                if num is not None:
                    total += num
            else:
                total += _recursive_sum_fee_fields(value)

    elif isinstance(data, list):
        for item in data:
            total += _recursive_sum_fee_fields(item)

    return total


def _extract_fee_total_usd(doc: Dict[str, Any]) -> float:
    metrics = doc.get("metrics") or {}

    explicit = _first_numeric(metrics, [
        ["fee_usd"],
        ["fees_usd"],
        ["collected_fee_usd"],
        ["collected_fees_usd"],
        ["total_fee_usd"],
        ["total_fees_usd"],
        ["realized_fee_usd"],
        ["realized_fees_usd"],
        ["gross_fee_usd"],
        ["gross_fees_usd"],
        ["net_fee_usd"],
        ["net_fees_usd"],
        ["fees", "usd"],
        ["fee", "usd"],
        ["collected_fees", "usd"],
        ["total_fees", "usd"],
    ])
    if explicit is not None:
        return explicit

    recursive = _recursive_sum_fee_fields(metrics)
    return float(recursive or 0.0)


def _extract_gas_total_usd(doc: Dict[str, Any]) -> float:
    metrics = doc.get("metrics") or {}

    explicit = _first_numeric(metrics, [
        ["gas_cost_usd"],
        ["total_gas_usd"],
        ["gas", "cost_usd"],
    ])
    if explicit is not None:
        return explicit

    execution_log = doc.get("execution_log") or []
    if not isinstance(execution_log, list):
        return 0.0

    total = 0.0
    seen_tx_hashes = set()

    for item in execution_log:
        if not isinstance(item, dict):
            continue

        tx_hash = (
            _get_path(item, ["response", "tx_hash"])
            or _get_path(item, ["tx_hash"])
            or _get_path(item, ["response", "receipt", "transactionHash"])
        )

        gas_usd = _first_numeric(item, [
            ["response", "gas", "cost_usd"],
            ["gas", "cost_usd"],
            ["gas_cost_usd"],
        ])

        if gas_usd is None:
            continue

        key = str(tx_hash or f"no_hash_{len(seen_tx_hashes)}")
        if key in seen_tx_hashes:
            continue

        seen_tx_hashes.add(key)
        total += gas_usd

    return float(total or 0.0)


def _episode_time_for_recent_window(doc: Dict[str, Any]) -> Optional[int]:
    for field in ("close_time", "updated_at", "open_time", "created_at"):
        val = _safe_int(doc.get(field))
        if val is not None and val > 0:
            return val
    return None


class StrategyEpisodeRepositoryMongoDB(StrategyEpisodeRepository):
    COLLECTION = "strategy_episodes"
    RUNTIME_COLLECTION = "strategy_episode_runtimes"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db[self.COLLECTION]
        self._runtime_col = db[self.RUNTIME_COLLECTION]

    async def ensure_indexes(self) -> None:
        await self._col.create_index([("strategy_id", 1), ("status", 1)], name="ix_strategy_status")
        await self._col.create_index([("strategy_id", 1), ("open_time", -1)], name="ix_strategy_open_time")
        await self._col.create_index([("dex", 1), ("alias", 1), ("open_time", -1)], name="ix_dex_alias_open_time")
        await self._col.create_index([("dex", 1), ("alias", 1), ("status", 1), ("open_time", -1)], name="ix_dex_alias_status_open_time")

        await self._runtime_col.create_index(
            [("strategy_id", 1), ("ts", 1)],
            unique=True,
            name="ux_strategy_episode_runtime_strategy_ts",
        )
        await self._runtime_col.create_index(
            [("episode_id", 1), ("ts", -1)],
            name="ix_strategy_episode_runtime_episode_ts_desc",
        )
        await self._runtime_col.create_index(
            [("strategy_id", 1), ("ts", -1)],
            name="ix_strategy_episode_runtime_strategy_ts_desc",
        )
        await self._runtime_col.create_index(
            [("stream_key", 1), ("ts", -1)],
            name="ix_strategy_episode_runtime_stream_ts_desc",
        )

    async def get_open_by_strategy(self, strategy_id: str) -> Optional[StrategyEpisodeEntity]:
        doc = await self._col.find_one({"strategy_id": strategy_id, "status": "OPEN"})
        return StrategyEpisodeEntity.from_mongo(doc)

    async def open_new(self, episode: StrategyEpisodeEntity) -> StrategyEpisodeEntity:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        payload = episode.model_copy(update={
            "status": "OPEN",
            "created_at": now_ms,
            "created_at_iso": now_iso,
            "updated_at": now_ms,
            "out_above_streak": episode.out_above_streak or 0,
            "out_below_streak": episode.out_below_streak or 0,
        })
        mongo_doc = payload.to_mongo()
        await self._col.insert_one(mongo_doc)
        inserted = await self._col.find_one({"_id": mongo_doc["_id"]})
        return StrategyEpisodeEntity.from_mongo(inserted)

    async def close_episode(self, episode_id: str, close_fields: dict) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        await self._col.update_one(
            {"_id": episode_id},
            {"$set": {**close_fields, "status": "CLOSED", "updated_at": now_ms, "updated_at_iso": now_iso}},
        )

    async def update_partial(self, episode_id: str, partial: dict) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        await self._col.update_one(
            {"_id": episode_id},
            {"$set": {**partial, "updated_at": now_ms, "updated_at_iso": now_iso}},
        )

    async def list_by_strategy(self, strategy_id: str, limit: int = 50) -> List[StrategyEpisodeEntity | None]:
        cursor = self._col.find({"strategy_id": strategy_id}, sort=[("open_time", -1)], limit=limit)
        docs = await cursor.to_list(length=limit)
        return [StrategyEpisodeEntity.from_mongo(d) for d in docs if d]

    async def append_execution_log(self, episode_id: str, log: Dict[str, Any]) -> None:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        final_log = {
            "ts": now_ms,
            "ts_iso": now_iso,
            **log,
        }

        await self._col.update_one(
            {"_id": episode_id},
            {"$push": {"execution_log": final_log}},
        )

    async def upsert_runtime(self, runtime: StrategyEpisodeRuntimeEntity) -> StrategyEpisodeRuntimeEntity:
        now_ms = int(time.time() * 1000)
        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        payload = runtime.to_mongo()
        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        await self._runtime_col.update_one(
            {"strategy_id": runtime.strategy_id, "ts": int(runtime.ts)},
            {
                "$set": payload,
                "$setOnInsert": {
                    "created_at": now_ms,
                    "created_at_iso": now_iso,
                },
            },
            upsert=True,
        )

        stored = await self._runtime_col.find_one(
            {"strategy_id": runtime.strategy_id, "ts": int(runtime.ts)}
        )
        return StrategyEpisodeRuntimeEntity.from_mongo(stored)

    async def list_runtime_by_episode(
        self,
        episode_id: str,
        limit: int = 500,
        offset: int = 0,
    ) -> List[StrategyEpisodeRuntimeEntity | None]:
        cursor = (
            self._runtime_col.find({"episode_id": str(episode_id)})
            .sort([("ts", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        return [StrategyEpisodeRuntimeEntity.from_mongo(d) for d in docs if d]

    async def list_runtime_by_strategy(
        self,
        strategy_id: str,
        limit: int = 500,
        offset: int = 0,
    ) -> List[StrategyEpisodeRuntimeEntity | None]:
        cursor = (
            self._runtime_col.find({"strategy_id": str(strategy_id)})
            .sort([("ts", -1), ("created_at", -1)])
            .skip(int(offset))
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=int(limit))
        return [StrategyEpisodeRuntimeEntity.from_mongo(d) for d in docs if d]

    async def list_by_vault(
        self,
        *,
        dex: str,
        alias: str,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[StrategyEpisodeEntity | None]:
        q: Dict[str, Any] = {"dex": _norm(dex), "alias": (alias or "").strip()}
        st = _norm_status(status)
        if st:
            q["status"] = st

        cursor = self._col.find(q).sort("open_time", -1).skip(int(offset or 0)).limit(int(limit or 50))
        docs = await cursor.to_list(length=int(limit or 50))
        return [StrategyEpisodeEntity.from_mongo(d) for d in docs if d]

    async def count_by_vault(self, *, dex: str, alias: str, status: Optional[str] = None) -> int:
        q: Dict[str, Any] = {"dex": _norm(dex), "alias": (alias or "").strip()}
        st = _norm_status(status)
        if st:
            q["status"] = st
        return int(await self._col.count_documents(q))

    async def summarize_by_vault_refs(self, *, refs: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        if not refs:
            return []

        ordered_keys: List[str] = []
        query_or: List[Dict[str, str]] = []

        for ref in refs:
            dex = _norm(ref.get("dex"))
            alias = (ref.get("alias") or "").strip()
            if not dex or not alias:
                continue

            key = f"{dex}::{alias}"
            ordered_keys.append(key)
            query_or.append({"dex": dex, "alias": alias})

        if not query_or:
            return []

        docs = await self._col.find({"$or": query_or}).to_list(length=None)

        now_ms = int(time.time() * 1000)
        last_24h_ms = now_ms - 86_400_000

        summaries: Dict[str, Dict[str, Any]] = {}
        for ref in refs:
            dex = _norm(ref.get("dex"))
            alias = (ref.get("alias") or "").strip()
            if not dex or not alias:
                continue

            summaries[f"{dex}::{alias}"] = {
                "dex": dex,
                "alias": alias,
                "total_episodes": 0,
                "open_episodes": 0,
                "closed_episodes": 0,
                "has_open_episode": False,
                "latest_status": None,
                "fee_total_usd": 0.0,
                "fee_24h_usd": 0.0,
                "gas_total_usd": 0.0,
                "gas_24h_usd": 0.0,
                "latest_open_time": None,
                "latest_open_time_iso": None,
                "latest_close_time": None,
                "latest_close_time_iso": None,
                "latest_updated_at": None,
                "latest_updated_at_iso": None,
            }

        for doc in docs:
            dex = _norm(doc.get("dex"))
            alias = (doc.get("alias") or "").strip()
            key = f"{dex}::{alias}"

            if key not in summaries:
                continue

            summary = summaries[key]
            status = str(doc.get("status") or "").upper()

            summary["total_episodes"] += 1
            if status == "OPEN":
                summary["open_episodes"] += 1
                summary["has_open_episode"] = True
            elif status == "CLOSED":
                summary["closed_episodes"] += 1

            updated_at = _safe_int(doc.get("updated_at"))
            if updated_at is None:
                updated_at = _safe_int(doc.get("open_time"))

            current_latest = _safe_int(summary.get("latest_updated_at"))
            if updated_at is not None and (current_latest is None or updated_at > current_latest):
                summary["latest_updated_at"] = updated_at
                summary["latest_updated_at_iso"] = doc.get("updated_at_iso")
                summary["latest_status"] = status or None

            open_time = _safe_int(doc.get("open_time"))
            current_open = _safe_int(summary.get("latest_open_time"))
            if open_time is not None and (current_open is None or open_time > current_open):
                summary["latest_open_time"] = open_time
                summary["latest_open_time_iso"] = doc.get("open_time_iso")

            close_time = _safe_int(doc.get("close_time"))
            current_close = _safe_int(summary.get("latest_close_time"))
            if close_time is not None and (current_close is None or close_time > current_close):
                summary["latest_close_time"] = close_time
                summary["latest_close_time_iso"] = doc.get("close_time_iso")

            fee_total_usd = _extract_fee_total_usd(doc)
            gas_total_usd = _extract_gas_total_usd(doc)

            summary["fee_total_usd"] += fee_total_usd
            summary["gas_total_usd"] += gas_total_usd

            event_ts = _episode_time_for_recent_window(doc)
            if event_ts is not None and event_ts >= last_24h_ms:
                summary["fee_24h_usd"] += fee_total_usd
                summary["gas_24h_usd"] += gas_total_usd

        return [summaries[key] for key in ordered_keys if key in summaries]