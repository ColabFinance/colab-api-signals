from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from typing import Any, Optional

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.notify.telegram_notifier import TelegramNotifier
from adapters.external.trade_execution.trade_execution_http_client import TradeExecutionHttpClient
from core.domain.entities.trade_signal_entity import TradeSignalEntity
from core.domain.enums.trade_enums import TradeSignalType
from core.repositories.trade_signal_repository import TradeSignalRepository
from core.repositories.trade_strategy_runtime_snapshot_repository import (
    TradeStrategyRuntimeSnapshotRepository,
)


class ExecuteTradeSignalPipelineUseCase:
    """
    Consume pending trade signals and execute them through api-trade-execution.
    """

    def __init__(
        self,
        *,
        trade_signal_repo: TradeSignalRepository,
        runtime_snapshot_repo: TradeStrategyRuntimeSnapshotRepository,
        trade_execution_client: TradeExecutionHttpClient,
        market_data_client: Optional[MarketDataHttpClient] = None,
        notifier: Optional[TelegramNotifier] = None,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 5,
        base_backoff_sec: float = 1.0,
        max_parallel: int = 4,
    ) -> None:
        """
        Initialize the trade signal execution pipeline use case.
        """
        self._trade_signal_repo = trade_signal_repo
        self._runtime_snapshot_repo = runtime_snapshot_repo
        self._trade_execution_client = trade_execution_client
        self._market_data = market_data_client or MarketDataHttpClient.from_settings()
        self._notifier = notifier
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._max_retries = int(max_retries)
        self._base_backoff_sec = float(base_backoff_sec)
        self._max_parallel = int(max_parallel)
        self._semaphore = asyncio.Semaphore(self._max_parallel)
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def execute_once(self) -> bool:
        """
        Fetch pending trade signals and process them.

        Returns:
            True when at least one signal was found.
            False when no work was available.
        """
        pending = await self._trade_signal_repo.list_pending(limit=50)
        if not pending:
            return False

        tasks = [self._run_single_with_lock(signal) for signal in pending]
        await asyncio.gather(*tasks, return_exceptions=False)
        return True

    async def _run_single_with_lock(self, signal: TradeSignalEntity) -> None:
        """
        Process one trade signal with semaphore and per-symbol/account locking.
        """
        lock_key = self._build_lock_key(signal)
        lock = await self._get_lock(lock_key)

        async with self._semaphore:
            async with lock:
                try:
                    await self._process_signal(signal)
                except Exception as exc:
                    self._logger.exception("Unexpected trade signal execution error: %s", exc)
                    await self._trade_signal_repo.mark_failure(signal, str(exc))
                    await self._notify_failure(signal, f"UNEXPECTED: {exc}")

    async def _process_signal(self, signal: TradeSignalEntity) -> bool:
        """
        Execute one trade signal with retries.
        """
        execution_account_id = str(signal.payload.get("execution_account_id") or "").strip()
        if not execution_account_id:
            await self._trade_signal_repo.mark_failure(signal, "execution_account_id is missing in signal payload")
            await self._notify_failure(signal, "execution_account_id is missing in signal payload")
            return False

        last_error: Optional[str] = None

        for attempt in range(self._max_retries):
            try:
                response = await self._dispatch_signal(signal, execution_account_id)
                await self._trade_signal_repo.mark_success(signal, execution_response=response)
                await self._notify_success(signal, response)
                return True
            except Exception as exc:
                last_error = str(exc)
                self._logger.warning(
                    "Trade signal execution failed. signal_id=%s attempt=%s/%s err=%s",
                    signal.id,
                    attempt + 1,
                    self._max_retries,
                    exc,
                )

                if self._is_non_retryable_error(last_error):
                    break

                await asyncio.sleep(self._base_backoff_sec * (attempt + 1))

        await self._trade_signal_repo.mark_failure(signal, last_error or "unknown trade execution error")
        await self._notify_failure(signal, last_error or "unknown trade execution error")
        return False

    async def _dispatch_signal(
        self,
        signal: TradeSignalEntity,
        execution_account_id: str,
    ) -> dict:
        """
        Dispatch one trade signal to the appropriate api-trade-execution endpoint.
        """
        signal_type = self._normalize_signal_type(signal.signal_type)

        if signal_type in {TradeSignalType.OPEN_LONG, TradeSignalType.OPEN_SHORT}:
            position_side = "LONG" if signal_type == TradeSignalType.OPEN_LONG else "SHORT"
            return await self._trade_execution_client.open_position(
                strategy_id=str(signal.strategy_id),
                execution_account_id=execution_account_id,
                symbol=str(signal.symbol).upper(),
                position_side=position_side,
                signal_id=str(signal.id) if signal.id else None,
                signal_ts=int(signal.ts),
                signal_type=signal_type.value,
                idempotency_key=str(signal.idempotency_key),
            )

        if signal_type in {
            TradeSignalType.CLOSE_LONG,
            TradeSignalType.CLOSE_SHORT,
            TradeSignalType.CLOSE_POSITION,
        }:
            return await self._trade_execution_client.close_position(
                strategy_id=str(signal.strategy_id),
                execution_account_id=execution_account_id,
                symbol=str(signal.symbol).upper(),
                close_reason=signal_type.value,
                signal_id=str(signal.id) if signal.id else None,
                signal_ts=int(signal.ts),
                idempotency_key=str(signal.idempotency_key),
            )

        raise RuntimeError(f"unsupported trade signal type: {signal_type}")

    async def _notify_success(self, signal: TradeSignalEntity, response: dict) -> None:
        """
        Send a success Telegram notification.

        The success text is always attempted first.
        Chart generation and chart upload are isolated so they can never
        suppress the main success message.
        """
        if self._notifier is None:
            return

        runtime = None
        runtime_error: Optional[str] = None
        try:
            runtime = await self._runtime_snapshot_repo.get_latest_by_strategy_id(str(signal.strategy_id))
        except Exception as exc:
            runtime_error = str(exc)
            self._logger.warning("Failed to load runtime snapshot for success notification: %s", exc)

        lines = [
            "✅ *Trade signal executado*",
            f"• Strategy ID: `{signal.strategy_id}`",
            f"• Symbol: `{signal.symbol}`",
            f"• Tipo: `{self._enum_value(signal.signal_type)}`",
            f"• Timestamp: `{signal.ts}`",
            f"• Execução: `{response.get('reason') or ('executed' if response.get('executed') else 'no-op')}`",
        ]

        if runtime is not None:
            lines.extend(
                [
                    "",
                    "*Runtime atual da estratégia*",
                    f"• STATE: `{self._enum_value(getattr(runtime, 'runtime_state', None))}`",
                    f"• ATR: `{runtime.atr}`",
                    f"• ATR_PCT: `{runtime.atr_pct}`",
                    f"• LOW_ATR_HIT: `{runtime.low_atr_hit}`",
                    f"• HIGH_ATR_HIT: `{runtime.high_atr_hit}`",
                    f"• SETUP_ARMED: `{runtime.setup_armed}`",
                    f"• SETUP_REFERENCE_PRICE: `{runtime.setup_reference_price}`",
                    f"• DESIRED_SIDE: `{self._enum_value(runtime.desired_side)}`",
                    f"• POSITION_SIDE: `{self._enum_value(runtime.position_side)}`",
                    f"• EVENT: `{self._enum_value(runtime.event)}`",
                    f"• BARS_SINCE_LAST_EVENT: `{getattr(runtime, 'bars_since_last_event', None)}`",
                    f"• CLOSE: `{runtime.close}`",
                ]
            )
        elif runtime_error:
            lines.extend(
                [
                    "",
                    "*Runtime atual da estratégia*",
                    f"• Runtime indisponível: `{runtime_error}`",
                ]
            )

        await self._send_text_safe("\n".join(lines))

        chart_path: Optional[str] = None
        try:
            chart_path = await self._build_chart_image(signal.stream_key)
        except Exception as exc:
            self._logger.warning("Trade chart generation failed: %s", exc)
            await self._send_text_safe(
                "\n".join(
                    [
                        "⚠️ *Trade executado, mas o gráfico não foi gerado*",
                        f"• Strategy ID: `{signal.strategy_id}`",
                        f"• Symbol: `{signal.symbol}`",
                        f"• Motivo: `{exc}`",
                    ]
                )
            )
            return

        if chart_path is not None:
            try:
                await self._notifier.send_photo(
                    chart_path,
                    caption=f"{signal.symbol} - {self._enum_value(signal.signal_type)}",
                )
            except Exception as exc:
                self._logger.warning("Trade success chart notification failed: %s", exc)
                await self._send_text_safe(
                    "\n".join(
                        [
                            "⚠️ *Trade executado, mas o envio do gráfico falhou*",
                            f"• Strategy ID: `{signal.strategy_id}`",
                            f"• Symbol: `{signal.symbol}`",
                            f"• Motivo: `{exc}`",
                        ]
                    )
                )
            finally:
                try:
                    os.remove(chart_path)
                except Exception:
                    pass

    async def _notify_failure(self, signal: TradeSignalEntity, error_message: str) -> None:
        """
        Send a failure Telegram notification.

        Notification failures are logged but never raise.
        """
        if self._notifier is None:
            return

        text = "\n".join(
            [
                "❌ *Falha ao executar trade signal*",
                f"• Strategy ID: `{signal.strategy_id}`",
                f"• Symbol: `{signal.symbol}`",
                f"• Tipo: `{self._enum_value(signal.signal_type)}`",
                f"• Timestamp: `{signal.ts}`",
                f"• Erro: `{error_message}`",
            ]
        )
        await self._send_text_safe(text)

    async def _send_text_safe(self, text: str) -> None:
        """
        Send one Telegram text safely.

        Errors are logged but never propagated.
        """
        if self._notifier is None:
            return

        try:
            await self._notifier.send_message(text)
        except Exception as exc:
            self._logger.warning("Telegram text notification failed: %s", exc)

    async def _build_chart_image(self, stream_key: str) -> Optional[str]:
        """
        Build a candlestick chart image using the latest candles from api-market-data.
        """
        candles = await self._market_data.list_candles(
            stream_key=str(stream_key).strip().lower(),
            limit=100,
        )
        if not candles:
            return None

        df = pd.DataFrame(candles).copy()
        required = {"open_time", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            return None

        df["Time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["Open"] = pd.to_numeric(df["open"], errors="coerce")
        df["High"] = pd.to_numeric(df["high"], errors="coerce")
        df["Low"] = pd.to_numeric(df["low"], errors="coerce")
        df["Close"] = pd.to_numeric(df["close"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df = df[["Time", "Open", "High", "Low", "Close", "Volume"]].dropna()
        if df.empty:
            return None

        df = df.set_index("Time")

        fd, path = tempfile.mkstemp(suffix=".png", prefix="trade_signal_chart_")
        os.close(fd)

        fig, ax = plt.subplots(figsize=(10, 6))
        mpf.plot(df.tail(100), type="candle", ax=ax)
        plt.savefig(path)
        plt.close(fig)

        return path

    def _build_lock_key(self, signal: TradeSignalEntity) -> str:
        """
        Build the per-symbol/account lock key.
        """
        execution_account_id = str(signal.payload.get("execution_account_id") or "?").strip()
        return f"{execution_account_id}:{str(signal.symbol).upper()}"

    async def _get_lock(self, key: str) -> asyncio.Lock:
        """
        Return or create one asyncio lock for a specific key.
        """
        async with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    def _is_non_retryable_error(self, error_message: str) -> bool:
        """
        Check whether an execution error is deterministic and should not be retried.
        """
        raw = str(error_message or "").lower()

        non_retryable_patterns = [
            "active position exists with different side",
            "execution profile not found",
            "execution profile is disabled",
            "execution_account_id is missing",
            "unsupported trade signal type",
            "minimum notional",
            "below exchange minimum",
            "order's notional must be no smaller than",
            "quote_size_usd below exchange minimum notional",
            "normalized order notional below exchange minimum",
        ]
        return any(pattern in raw for pattern in non_retryable_patterns)

    def _normalize_signal_type(self, value: Any) -> TradeSignalType:
        """
        Normalize a stored signal type into the enum.
        """
        if isinstance(value, TradeSignalType):
            return value
        return TradeSignalType(str(value).strip().upper())

    def _enum_value(self, value: Any) -> Any:
        """
        Return enum value when the object is an enum, otherwise return it unchanged.
        """
        return value.value if hasattr(value, "value") else value