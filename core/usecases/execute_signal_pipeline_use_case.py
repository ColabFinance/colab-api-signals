import asyncio
from datetime import datetime, timezone
import json
import logging
from math import sqrt
from typing import Any, Dict, List, Optional, Tuple

from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.notify.telegram_notifier import TelegramNotifier
from core.common.utils import sanitize_for_bson
from core.domain.entities.signal_entity import SignalEntity
from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity
from core.repositories.strategy_episode_repository import StrategyEpisodeRepository
from core.services.idempotency_key_service import IdempotencyKeyService
from core.usecases.evaluate_active_strategies_use_case import EvaluateActiveStrategiesUseCase

from ..repositories.signal_repository import SignalRepository
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient


USD_SET = {"USDC","USDBC","USDCE","USDT","DAI","USDD","USDP","BUSD"}


class ExecuteSignalPipelineUseCase:
    """
    Consumes PENDING signals from Mongo and executes their steps IN ORDER
    via PipelineHttpClient (api-liquidity-provider).

    Rules:
      - Steps = [COLLECT, WITHDRAW, SWAP_EXACT_IN, OPEN] (some may be skipped).
      - Each step retries with backoff.
      - On hard failure -> mark FAILED and stop processing that signal.
      - On full success -> mark SENT.

    Runtime sizing logic:
      - before SWAP_EXACT_IN we read /status to pick direction/amount.
      - before OPEN we read /status again to snapshot idle caps.
    """

    def __init__(
        self,
        signal_repo: SignalRepository,
        episode_repo: StrategyEpisodeRepository,
        lp_client: PipelineHttpClient,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 5,
        base_backoff_sec: float = 1.0,
        notifier: Optional[TelegramNotifier] = None,
        idempotency_service: Optional[IdempotencyKeyService] = None,
        max_parallel: int = 4,
        market_data_client: Optional[MarketDataHttpClient] = None,
    ):
        self._signal_repo = signal_repo
        self._episode_repo = episode_repo
        self._lp = lp_client
        self._market_data = market_data_client or MarketDataHttpClient.from_settings()

        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._max_retries = max_retries
        self._base_backoff = base_backoff_sec
        self._notifier = notifier
        self._idempotency = idempotency_service or IdempotencyKeyService()
        self._locks: dict[str, asyncio.Lock] = {}

        self._locks_lock = asyncio.Lock()  # pra criar locks de forma segura
        self._max_parallel = max_parallel
        self._semaphore = asyncio.Semaphore(self._max_parallel)
        self.EPS_POS = 1e-12  # usado para clamps de raiz e separação Pa<P<Pb
     
    # -------------------------
    # Tick APR helpers
    # -------------------------

    def _parse_ts_ms(self, v: Any) -> Optional[int]:
        """
        Accepts:
          - ISO string (with Z or +00:00)
          - unix ms int
          - unix seconds int
        Returns ms since epoch (UTC).
        """
        if v is None:
            return None

        if isinstance(v, (int, float)):
            iv = int(v)
            # seconds range (10 digits) vs ms range (13 digits)
            if iv < 10_000_000_000:
                return iv * 1000
            return iv

        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                s2 = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s2)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None

        return None

    def _seconds_in_range_from_ticks(
        self,
        ticks: List[Tuple[int, float]],
        Pa: float,
        Pb: float,
        invert_price: bool,
    ) -> float:
        if not ticks or len(ticks) < 2:
            return 0.0
        if Pa > Pb:
            Pa, Pb = Pb, Pa

        sec_in = 0.0
        for (ts0, p0), (ts1, p1) in zip(ticks, ticks[1:]):
            dt = max(0.0, (int(ts1) - int(ts0)) / 1000.0)
            if dt <= 0.0:
                continue

            a = float(p0)
            b = float(p1)

            # bring to "human USD-per-risk" if needed
            if invert_price:
                if a > 0:
                    a = 1.0 / a
                if b > 0:
                    b = 1.0 / b

            pm = (a + b) / 2.0
            if Pa < pm < Pb:
                sec_in += dt

        return float(sec_in)

    def _apr_annual_pct_from_minutes(self, fees_usd: float, base_usd: float, minutes_in_range: float) -> float:
        if fees_usd <= 0.0 or base_usd <= 0.0 or minutes_in_range <= 0.0:
            return 0.0
        minutes_per_year = 525_600.0
        return float((fees_usd / base_usd) * (minutes_per_year / minutes_in_range) * 100.0)

    def _apr_annual_pct_from_seconds(self, fees_usd: float, base_usd: float, seconds_in_range: float) -> float:
        if fees_usd <= 0.0 or base_usd <= 0.0 or seconds_in_range <= 0.0:
            return 0.0
        seconds_per_year = 365.0 * 24.0 * 3600.0
        return float((fees_usd / base_usd) * (seconds_per_year / seconds_in_range) * 100.0)

    async def _fetch_ticks_paginated(
        self,
        *,
        stream_key: str,
        ts_from: int,
        ts_to: int,
        hard_limit: int = 200_000,
        page_limit: int = 50_000,
    ) -> List[Dict[str, Any]]:
        """
        Paginates via ts_from to avoid missing long episodes.
        Uses /market-data/price-ticks which returns sorted ticks by ts.
        """
        out: List[Dict[str, Any]] = []
        cursor = int(ts_from)
        loops = 0

        while cursor <= int(ts_to) and len(out) < int(hard_limit):
            loops += 1
            if loops > 25:
                break

            lim = int(min(page_limit, hard_limit - len(out)))
            batch = await self._market_data.list_price_ticks(
                stream_key=stream_key,
                ts_from=cursor,
                ts_to=int(ts_to),
                limit=lim,
                access_token=None,
            )
            if not batch:
                break

            out.extend(batch)

            last_ts = None
            try:
                last_ts = int(batch[-1].get("ts"))
            except Exception:
                last_ts = None

            if last_ts is None or last_ts <= cursor:
                break

            if len(batch) < lim:
                break

            cursor = last_ts + 1

        # ensure sorted & unique-ish
        out.sort(key=lambda x: int(x.get("ts") or 0))
        return out
   
    def _build_idempotency_key(self, signal: SignalEntity, action: str) -> str:
        return self._idempotency.build_for_signal_step(signal, action)
    
    async def _notify_telegram(self, text: str) -> None:
        """
        Helper para enviar mensagem Telegram (se configurado).
        Não deixa exceção do notifier quebrar o fluxo.
        """
        if not self._notifier:
            return
        try:
            await self._notifier.send_message(text)
        except Exception as exc:
            self._logger.warning("Failed to send Telegram message: %s", exc)
            
    def _tokens_from_L(self, L, Pa, Pb, P):
        xa, xb, x = sqrt(Pa), sqrt(Pb), sqrt(P)
        if P <= Pa:   # tudo vira token0
            t0 = L * (1/xa - 1/xb); t1 = 0
        elif P >= Pb: # tudo vira token1
            t0 = 0; t1 = L * (xb - xa)
        else:         # misto
            t0 = L * (1/x - 1/xb)
            t1 = L * (x - xa)
        return t0, t1

    def _ensure_valid_band(self, Pa: float, Pb: float, P: float) -> Tuple[float, float]:
        """
        Garante:
        - Pa >= EPS_POS
        - Pb >= Pa + EPS_POS
        - Banda não degenera no mid (respeita almofada mínima em torno de P)
        """
        Pa = max(self.EPS_POS, Pa)
        Pb = max(Pa + self.EPS_POS, Pb)

        # almofada mínima ao redor de P (como você já faz em alguns pontos)
        mid_pad = self.EPS_POS * max(1.0, P)
        Pa = min(P - mid_pad, Pa)
        Pb = max(P + mid_pad, Pb)

        # Se por alguma razão Pa ultrapassou Pb após clamps, corrige separando pelo mid_pad:
        if not (Pa < Pb):
            Pa = P - mid_pad
            Pb = P + mid_pad

        return Pa, Pb

    def _is_usd(self, sym: str) -> bool:
        try:
            return (sym or "").upper() in USD_SET
        except Exception:
            return False
        
    def _L_closed(self, total_P: float, P: float, Pa: float, Pb: float) -> float:
        # assegura banda válida
        Pa, Pb = self._ensure_valid_band(Pa, Pb, P)

        a  = sqrt(max(self.EPS_POS, P))
        xa = sqrt(max(self.EPS_POS, Pa))
        xb = sqrt(max(self.EPS_POS, Pb))

        denom = 2 * a - xa - (P / xb)
        if denom <= 0:
            denom = self.EPS_POS
        return total_P / denom

    async def _compute_dynamic_range_after_status(
        self,
        st: Dict,
        episode: StrategyEpisodeEntity,
    ) -> Tuple[float, float, Dict[str, Any]]:
        """
        Uses the range already decided by the LP strategy.

        This is the critical change that decouples the executor from the old
        tier-based evaluator. The pipeline now trusts episode.Pa / episode.Pb
        and only derives price-scale context from live vault status.
        """
        prices = (st.get("prices") or {})
        cur = (prices.get("current") or {})
        p_t1_t0_spot = float(cur.get("p_t1_t0") or 0.0)
        if p_t1_t0_spot <= 0.0:
            raise RuntimeError("spot_price_unavailable")
        p_t0_t1_spot = 1.0 / p_t1_t0_spot

        holdings = (st.get("holdings") or {})
        syms = (holdings.get("symbols") or {})
        sym0 = (syms.get("token0") or "").upper()
        sym1 = (syms.get("token1") or "").upper()

        token0_is_usd = self._is_usd(sym0)
        token1_is_usd = self._is_usd(sym1)

        if token0_is_usd and not token1_is_usd:
            P_h = p_t0_t1_spot
            human_is_t0_t1 = True
        else:
            P_h = p_t1_t0_spot
            human_is_t0_t1 = False

        Pa_h = float(episode.Pa)
        Pb_h = float(episode.Pb)
        if Pa_h > Pb_h:
            Pa_h, Pb_h = Pb_h, Pa_h

        ctx = {
            "P_h": float(P_h),
            "human_is_t0_t1": bool(human_is_t0_t1),
            "token0_is_usd": bool(token0_is_usd),
            "token1_is_usd": bool(token1_is_usd),
            "p_t1_t0_spot": float(p_t1_t0_spot),
            "p_t0_t1_spot": float(p_t0_t1_spot),
            "open_side": episode.open_side or episode.mode_on_open,
            "range_width_pct": episode.range_width_pct,
            "range_width_regime": episode.range_width_regime,
        }
        return Pa_h, Pb_h, ctx
    
    async def execute_once(self) -> bool:
        """
        Fetch up to N pending signals and attempt to execute them.
        """
        pending: List[SignalEntity] = await self._signal_repo.list_pending(limit=50)
        if not pending:
            return False
        
        tasks = []
        for sig in pending:
            tasks.append(self._run_single_with_locks(sig))
        
        # executa tudo em paralelo, respeitando semaphore global
        await asyncio.gather(*tasks, return_exceptions=False)
        
        return True
    
    async def _run_single_with_locks(self, sig: SignalEntity) -> None:
        """
        Envolve _process_single_signal com:
          - semaphore global (max_parallel)
          - lock por vault (dex+alias)
        """
        episode = sig.episode
        dex = episode.dex
        alias = episode.alias

        vault_lock = await self._get_vault_lock(dex, alias)

        async with self._semaphore:
            async with vault_lock:
                try:
                    ok = await self._process_single_signal(sig)
                    if ok:
                        await self._signal_repo.mark_success(sig)
                    # if not ok, _process_single_signal already marked FAILED
                except Exception as exc:
                    self._logger.exception("Unexpected error processing signal %s: %s", sig, exc)
                    msg = f"UNEXPECTED: {exc}"
                    await self._signal_repo.mark_failure(sig, msg)

                    try:
                        sig_id = sig.id

                        msg_lines = [
                            "🔥 *Erro inesperado ao processar sinal*",
                            f"• Sinal: `{sig_id}`",
                            f"• DEX: `{dex}`",
                            f"• Vault: `{alias}`",
                            f"• Erro: `{msg}`",
                        ]
                        await self._notify_telegram("\n".join(msg_lines))
                    except Exception:
                        pass
    
    async def _get_vault_lock(self, dex: Optional[str], alias: Optional[str]) -> asyncio.Lock:
        """
        Retorna um asyncio.Lock específico pra combinação (dex, alias).

        - Garante que nunca existam duas pipelines concorrentes pro MESMO vault.
        - Sinais de vaults diferentes rodam em paralelo (ETH vs BTC, etc.).
        - Se dex/alias vierem None, usamos um placeholder "?" só pra ter chave estável.
        """
        key = f"{dex or '?'}:{alias or '?'}"

        # garante criação thread-safe/async-safe do lock
        async with self._locks_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def _append_log(
        self,
        episode_id: Optional[str],
        base: Dict,
    ) -> None:
        """
        Helper: push a log line into the episode doc, if we have an episode_id.

        IMPORTANT:
        - Mongo only supports int64. Onchain payloads often contain huge ints (sqrtPriceX96 etc).
        - sanitize_for_bson must be applied to avoid BSON int overflow.
        """
        if not episode_id:
            return
        try:
            payload = sanitize_for_bson(base)
            await self._episode_repo.append_execution_log(episode_id, payload)
        except Exception as log_exc:
            self._logger.warning(
                "Failed to append_execution_log for %s: %s",
                episode_id,
                log_exc,
            )


    async def _process_single_signal(self, sig: SignalEntity) -> bool:
        """
        Executes a single signal's steps sequentially.
        Returns True on full success, False if FAILED.
        """
        steps = [s.model_dump(mode="python") for s in sig.steps]
        
        last_episode = sig.last_episode
        last_episode_id = (last_episode.id if last_episode else None)

        episode = sig.episode
        episode_id = episode.id

        dex = episode.dex
        alias = episode.alias
        token0_addr = episode.token0_address
        token1_addr = episode.token1_address
        majority_flag = episode.majority_on_open

        batch_res = None
        
        st = None
        for step in steps:
            action = step.get("action")
            self._logger.info("Executing step %s for %s/%s", action, dex, alias)
            
            idem_key = self._build_idempotency_key(sig, action)
            
            if (not dex or not alias) and action != "NOOP_LEGACY":
                skip_msg = "Skipping step because no dex/alias is wired for this strategy."
                self._logger.info("%s %s", action, skip_msg)
                
                await self._append_log(
                    episode_id,
                    {
                        "step": action,
                        "phase": "skipped_no_dex_alias",
                        "reason": skip_msg,
                    },
                )
                
                continue
            
            success = False
            last_err: Optional[str] = None

            for attempt in range(self._max_retries):
                try:
                    if action == "NOOP_LEGACY":
                        success = True
                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "noop",
                                "attempt": attempt + 1,
                                "info": "NOOP_LEGACY executed",
                            },
                        )
                    
                    
                    elif action == "BATCH_REQUEST":
                        # after withdraw, capital is idle in vault.
                        st = await self._lp.get_status(alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        lower_price, upper_price, rctx = await self._compute_dynamic_range_after_status(st, episode)
                        
                        holdings = st.get("holdings", {}) or {}
                        totals = holdings.get("totals", {}) or {}
                        amt0 = float(totals.get("token0", 0.0))
                        amt1 = float(totals.get("token1", 0.0))
                        
                        prices = st.get("prices", {}) or {}
                        cur = prices.get("current", {}) or {}
                        p_t1_t0_spot = float(cur.get("p_t1_t0", 0.0))    # canônico (token1 per token0)
                        p_t0_t1_spot = (0.0 if p_t1_t0_spot == 0.0 else 1.0 / p_t1_t0_spot)

                        syms = (holdings.get("symbols") or {})
                        sym0 = (syms.get("token0") or "").upper()
                        sym1 = (syms.get("token1") or "").upper()

                        addrs = (holdings.get("addresses") or {})
                        t0_addr = addrs.get("token0") or token0_addr
                        t1_addr = addrs.get("token1") or token1_addr
                        
                        token0_is_usd = self._is_usd(sym0)
                        token1_is_usd = self._is_usd(sym1)

                        
                        # ---------- Escala HUMANA: USDC por 1 RISCO quando há USD em um dos lados ----------
                        # Pa/Pb já chegam nessa escala humana; então ajustamos o spot para a MESMA escala:
                        Pa_h = float(lower_price)
                        Pb_h = float(upper_price)
                        if Pa_h <= 0 or Pb_h <= 0:
                            raise RuntimeError("Prices must be positive.")
                        if Pa_h > Pb_h:
                            Pa_h, Pb_h = Pb_h, Pa_h

                        P_h = float(rctx["P_h"])
                        if token0_is_usd and not token1_is_usd:
                            # par USDC/CRYPTO → humano é p_t0_t1
                            human_is_t0_t1 = True
                        elif token1_is_usd and not token0_is_usd:
                            # par CRYPTO/USDC → humano é p_t1_t0
                            human_is_t0_t1 = False
                        else:
                            # sem USD: mantemos humano como p_t1_t0
                            human_is_t0_t1 = False
                        
                        # ---------- Valoração em USD na escala humana ----------
                        # USD por unidade conforme a escala humana escolhida
                        if token1_is_usd:
                            usd_per_t0 = P_h     # se P_h=p_t1_t0, USD por token0 = P_h
                            usd_per_t1 = 1.0     # token1 já é USD
                        elif token0_is_usd:
                            usd_per_t0 = 1.0     # token0 já é USD
                            usd_per_t1 = P_h     # se P_h=p_t0_t1, USD por token1 = P_h
                        else:
                            # fallback: trate token1 como 'quote' (P_h ~ p_t1_t0)
                            usd_per_t0 = P_h
                            usd_per_t1 = 1.0

                        usd0 = amt0 * usd_per_t0
                        usd1 = amt1 * usd_per_t1
                        total_usd = usd0 + usd1

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "pre_calc",
                                "attempt": attempt + 1,
                                "holdings_raw": {"amt0": amt0, "amt1": amt1},
                                "symbols": {"sym0": sym0, "sym1": sym1},
                                "human_scale": {
                                    "P_h": P_h, "Pa_h": Pa_h, "Pb_h": Pb_h,
                                    "human_is_t0_t1": human_is_t0_t1
                                },
                                "canonical_spot": {
                                    "p_t1_t0_spot": p_t1_t0_spot,
                                    "p_t0_t1_spot": p_t0_t1_spot
                                },
                                "usd_per_token": {"usd_per_t0": usd_per_t0, "usd_per_t1": usd_per_t1},
                                "valuation": {"usd0": usd0, "usd1": usd1, "total_usd": total_usd},
                            },
                        )
                        
                        # --------------- Conversão p/ CANÔNICO nas fórmulas internas ---------------
                        # Pa/Pb/P sempre em p_t1_t0 para _L_closed/_tokens_from_L
                        if human_is_t0_t1:
                            # humano (Pa_h,Pb_h) = p_t0_t1; converter para p_t1_t0
                            Pa_c = 1.0 / Pa_h
                            Pb_c = 1.0 / Pb_h
                            if Pa_c > Pb_c:
                                Pa_c, Pb_c = Pb_c, Pa_c
                            P_c = p_t1_t0_spot
                        else:
                            Pa_c, Pb_c, P_c = Pa_h, Pb_h, p_t1_t0_spot

                        # --------------- Liquidez alvo + AUTO-CALIBRAÇÃO ---------------
                        # 1) L provisório:
                        L_target_guess = self._L_closed(total_usd, P_c, Pa_c, Pb_c)

                        # 2) Tokens com L provisório:
                        t0_g, t1_g = self._tokens_from_L(L_target_guess, Pa_c, Pb_c, P_c)

                        # 3) Valuation desses tokens:
                        value_guess_usd = t0_g * usd_per_t0 + t1_g * usd_per_t1
                        scale = 1.0
                        if value_guess_usd > 1e-18:
                            scale = total_usd / value_guess_usd

                        # 4) L final calibrado e tokens finais coerentes com total_usd:
                        L_target = L_target_guess * scale
                        t0_needed, t1_needed = self._tokens_from_L(L_target, Pa_c, Pb_c, P_c)
                        value_final_usd = t0_needed * usd_per_t0 + t1_needed * usd_per_t1

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "calc_tokens",
                                "attempt": attempt + 1,
                                "canonical_prices": {"Pa_c": Pa_c, "Pb_c": Pb_c, "P_c": P_c},
                                "liquidity": {
                                    "L_guess": L_target_guess,
                                    "value_guess_usd": value_guess_usd,
                                    "scale": scale,
                                    "L_final": L_target,
                                    "value_final_usd": value_final_usd,
                                    "total_usd": total_usd
                                },
                                "tokens_needed": {"t0_needed": t0_needed, "t1_needed": t1_needed}
                            },
                        )
                        
                        # ---------- Lados (USD vs risco) ----------
                        usd_side  = 0 if token0_is_usd else (1 if token1_is_usd else 1)
                        risk_side = 1 - usd_side

                        # USD atual em cada lado (na escala humana)
                        risk_usd_now = usd1 if risk_side == 1 else usd0
                        usd_usd_now  = usd1 if usd_side  == 1 else usd0

                        # USD necessário em cada lado (na escala humana)
                        if risk_side == 1:
                            risk_needed_usd = t1_needed * usd_per_t1
                            usd_needed_usd  = t0_needed * usd_per_t0
                        else:
                            risk_needed_usd = t0_needed * usd_per_t0
                            usd_needed_usd  = t1_needed * usd_per_t1

                        majority_flag = (episode.majority_on_open or "").lower()
                        align_usd = (majority_flag == "token1")

                        falta_usd = (usd_needed_usd - usd_usd_now) if align_usd else (risk_needed_usd - risk_usd_now)

                        # --------------- Encerrar cedo + log seguro ---------------
                        if abs(falta_usd) <= 1e-9:
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "skip_small",
                                    "attempt": attempt + 1,
                                    "reason": f"no meaningful delta ({'usd side' if align_usd else 'risk side'})",
                                    "post_tokens_needed": {"t0_needed": t0_needed, "t1_needed": t1_needed},
                                    "post_needed_usd": {"risk_needed_usd": risk_needed_usd, "usd_needed_usd": usd_needed_usd},
                                    "balances_usd": {"risk_usd_now": risk_usd_now, "usd_usd_now": usd_usd_now},
                                    "falta_usd": falta_usd
                                }
                            )
                            success = True
                            break
                        
                        # --------------- Escolha token_in/out e amount_in em UNIDADES do token_in ---------------
                        token_in_addr = ""
                        token_out_addr = ""
                        if align_usd:
                            if falta_usd > 0:
                                # comprar USD vendendo risco
                                token_in_addr  = t1_addr if risk_side == 1 else t0_addr
                                token_out_addr = t0_addr if usd_side  == 0 else t1_addr
                                usd_per_in     = usd_per_t1 if (risk_side == 1) else usd_per_t0
                            else:
                                # vender USD e comprar risco
                                token_in_addr  = t0_addr if usd_side  == 0 else t1_addr
                                token_out_addr = t1_addr if risk_side == 1 else t0_addr
                                usd_per_in     = usd_per_t0 if (usd_side == 0) else usd_per_t1
                        else:
                            # alinhar lado de risco
                            if falta_usd > 0:
                                # comprar RISCO vendendo USD
                                token_in_addr  = t0_addr if usd_side  == 0 else t1_addr
                                token_out_addr = t1_addr if risk_side == 1 else t0_addr
                                usd_per_in     = usd_per_t0 if (usd_side == 0) else usd_per_t1
                            else:
                                # vender RISCO e comprar USD
                                token_in_addr  = t1_addr if risk_side == 1 else t0_addr
                                token_out_addr = t0_addr if usd_side  == 0 else t1_addr
                                usd_per_in     = usd_per_t1 if (risk_side == 1) else usd_per_t0

                        amount_in_tokens = abs(falta_usd) / max(usd_per_in, 1e-18)
                        
                        # --------------- Teto por saldo disponível do token_in ---------------
                        bal_in_tokens = amt1 if (token_in_addr and token_in_addr.lower() == (t1_addr or "").lower()) else amt0
                        if amount_in_tokens > bal_in_tokens:
                            amount_in_tokens = bal_in_tokens

                        # margem minúscula para evitar “valor exato”
                        amount_in_tokens = max(0.0, amount_in_tokens - 1e-12)
                        
                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "calc_swap",
                                "attempt": attempt + 1,
                                "majority_flag": majority_flag,
                                "p_human": P_h,
                                "usd_per_token": {"usd_per_t0": usd_per_t0, "usd_per_t1": usd_per_t1},
                                "valuation": {"usd0": usd0, "usd1": usd1, "total_usd": total_usd},
                                "tokens_needed": {"t0_needed": t0_needed, "t1_needed": t1_needed},
                                "needed_usd": {"risk_needed_usd": risk_needed_usd, "usd_needed_usd": usd_needed_usd},
                                "balances_usd": {"risk_usd_now": risk_usd_now, "usd_usd_now": usd_usd_now},
                                "align_usd": align_usd,
                                "falta_usd": float(falta_usd),
                                "token_in": token_in_addr,
                                "token_out": token_out_addr,
                                "usd_per_in": usd_per_in,
                                "amount_in_tokens": amount_in_tokens,
                                "request_open": {"lower_price": lower_price, "upper_price": upper_price},
                                "liquidity_final": {"L_target": L_target, "Pa_c": Pa_c, "Pb_c": Pb_c, "P_c": P_c}
                            },
                        )

                        if amount_in_tokens <= 0.0:
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "skip_small",
                                    "attempt": attempt + 1,
                                    "reason": "no meaningful delta (post-margin)"
                                }
                            )
                            success = True
                            break

                        batch_res = await self._lp.post_auto_rebalance_pancake(
                            alias=alias,
                            lower_price=lower_price,
                            upper_price=upper_price,
                            token_in=token_in_addr,
                            token_out=token_out_addr,
                            swap_amount_in=amount_in_tokens,
                            swap_amount_out_min=0,
                            gas_strategy="buffered",
                            idempotency_key=idem_key,
                        )

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "swap_call",
                                "attempt": attempt + 1,
                                "request": {
                                    "token_in": token_in_addr,
                                    "token_out": token_out_addr,
                                    "amount_in": amount_in_tokens,
                                },
                                "request_open": {"lower_price": lower_price, "upper_price": upper_price},
                                "response": batch_res,
                            },
                        )
                        if batch_res is None:
                            raise RuntimeError("swap_failed")

                        try:
                            if episode_id:
                                await self._episode_repo.update_partial(
                                    episode_id,
                                    {
                                        "Pa": float(lower_price),
                                        "Pb": float(upper_price),
                                        "open_price_exec": float(P_h),
                                    },
                                )
                            episode.Pa = float(lower_price)
                            episode.Pb = float(upper_price)
                            episode.open_price_exec = float(P_h)
                        except Exception:
                            pass
                        
                        success = True
                                             
                    elif action == "OPEN":
                        # Antes de abrir nova faixa, snapshot de idle caps atuais
                        st2 = await self._lp.get_status(alias)
                        if not st2:
                            raise RuntimeError("status_unavailable_before_open")

                        hold2 = st2.get("holdings", {}) or {}
                        totals2 = hold2.get("totals", {}) or {}
                        cap0 = float(totals2.get("token0", 0.0))
                        cap1 = float(totals2.get("token1", 0.0))

                        lower_price, upper_price, rctx = await self._compute_dynamic_range_after_status(st2, episode)
                        P_h = float(rctx["P_h"])

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "pre_open",
                                "attempt": attempt + 1,
                                "idle_caps": {
                                    "cap0": cap0,
                                    "cap1": cap1,
                                },
                                "range": {
                                    "lower_price": lower_price,
                                    "upper_price": upper_price,
                                },
                                "range_ctx": {"P_h": P_h, "human_is_t0_t1": rctx.get("human_is_t0_t1")},
                            },
                        )

                        # Chamar o novo endpoint open
                        res = await self._lp.post_open(
                            dex=dex,
                            alias=alias,
                            lower_price=lower_price,
                            upper_price=upper_price,
                            lower_tick=None,
                            upper_tick=None,
                            idempotency_key=idem_key,
                        )

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "open_call",
                                "attempt": attempt + 1,
                                "request": {
                                    "lower_price": lower_price,
                                    "upper_price": upper_price,
                                },
                                "response": res,
                            },
                        )

                        if res is None:
                            raise RuntimeError("open_failed")
                        
                        try:
                            if episode_id:
                                await self._episode_repo.update_partial(
                                    episode_id,
                                    {
                                        "Pa": float(lower_price),
                                        "Pb": float(upper_price),
                                        "open_price_exec": float(P_h),
                                    },
                                )
                            episode.Pa = float(lower_price)
                            episode.Pb = float(upper_price)
                            episode.open_price_exec = float(P_h)
                        except Exception:
                            pass
                        
                        success = True
                    
                    else:
                        raise RuntimeError(f"unknown action {action}")

                    if success:
                        break

                except Exception as exc:
                    last_err = str(exc)
                    self._logger.warning(
                        "Step %s failed on attempt %s/%s: %s",
                        action, attempt + 1, self._max_retries, exc,
                    )
                    
                    await self._append_log(
                        episode_id,
                        {
                            "step": action,
                            "phase": "attempt_fail",
                            "attempt": attempt + 1,
                            "error": last_err,
                        },
                    )
                    
                    try:
                        dex_safe = dex or "?"
                        alias_safe = alias or "?"
                        msg_lines = [
                            "⚠️ *Falha ao executar step*",
                            f"• Step: `{action}`",
                            f"• DEX: `{dex_safe}`",
                            f"• Vault: `{alias_safe}`",
                            f"• Tentativa: `{attempt + 1}/{self._max_retries}`",
                            f"• Erro: `{last_err}`",
                        ]
                        # Marca explicitamente quando for a ÚLTIMA tentativa
                        if attempt + 1 >= self._max_retries:
                            msg_lines.append("")
                            msg_lines.append("🚨 *Última tentativa de retry atingida para este step.*")

                        await self._notify_telegram("\n".join(msg_lines))
                    except Exception:
                        # já é tratado dentro do _notify_telegram, então aqui é extra defensivo
                        pass
                    
                    # incremental backoff
                    await asyncio.sleep(self._base_backoff * (attempt + 1))

            if not success:
                # hard fail -> mark FAILED and stop this signal
                fail_msg = last_err or f"{action} failed"
                await self._signal_repo.mark_failure(sig, fail_msg)
                
                await self._append_log(
                    episode_id,
                    {
                        "step": action,
                        "phase": "hard_fail",
                        "error": fail_msg,
                    },
                )
                
                try:
                    dex_safe = dex or "?"
                    alias_safe = alias or "?"
                    sig_id = sig.id
                    msg_lines = [
                        "💥 *HARD FAIL ao processar sinal*",
                        f"• Sinal: `{sig_id}`",
                        f"• Step: `{action}`",
                        f"• DEX: `{dex_safe}`",
                        f"• Vault: `{alias_safe}`",
                        f"• Erro final: `{fail_msg}`",
                        "",
                        "Todos os retries foram esgotados para este step.",
                    ]
                    if episode_id:
                        msg_lines.append(f"• Episódio atual: `{episode_id}`")
                    if last_episode_id:
                        msg_lines.append(f"• Episódio anterior: `{last_episode_id}`")

                    await self._notify_telegram("\n".join(msg_lines))
                except Exception:
                    pass
                
                return False

        # all steps ok
        await self._append_log(
            episode_id,
            {
                "phase": "all_steps_done",
                "status": "SENT",
            },
        )
        
        # ==========================
        #  BLOCO DE MÉTRICAS + TELEGRAM
        # ==========================
        try:
            if batch_res and last_episode_id and last_episode:
                fees_uncollected_st = {}
                fees_uncollected_st_usd = 0.0
                gauge_rewards_st_usd = 0.0
                if st:
                    fees_uncollected_st = st.get("fees_uncollected") or {}
                    fees_uncollected_st_usd = float(fees_uncollected_st.get("usd", 0.0) or 0.0)
                    gauge_rewards_st = st.get("gauge_rewards") or {}
                    gauge_rewards_st_usd = float(gauge_rewards_st.get("pending_usd_est", 0.0) or 0.0)

                pending_cake = 0.0
                pending_cake_usd_est = 0.0
                if st:
                    gauge_rewards = st.get("gauge_rewards") or {}
                    pending_cake = float(gauge_rewards.get("pending_amount") or 0.0)
                    pending_cake_usd_est = float(gauge_rewards.get("pending_usd_est") or 0.0)

                p_t1_t0 = 0.0
                p_t0_t1 = 0.0
                if st:
                    prices = (st.get("prices") or {})
                    current_prices = (prices.get("current") or {})
                    p_t1_t0 = float(current_prices.get("p_t1_t0") or 0.0)
                    p_t0_t1 = float(current_prices.get("p_t0_t1") or (0.0 if p_t1_t0 == 0.0 else 1.0 / p_t1_t0))

                st_for_prices = await self._lp.get_status(alias)
                holdings_full = (st_for_prices or {}).get("holdings", {}) or {}
                syms = (holdings_full.get("symbols") or {})
                sym0 = (syms.get("token0") or "").upper()
                sym1 = (syms.get("token1") or "").upper()

                token0_is_usd = self._is_usd(sym0)
                token1_is_usd = self._is_usd(sym1)

                if token1_is_usd:
                    usd_per_t0 = p_t1_t0
                    usd_per_t1 = 1.0
                elif token0_is_usd:
                    usd_per_t0 = 1.0
                    usd_per_t1 = p_t0_t1
                else:
                    usd_per_t0 = p_t1_t0
                    usd_per_t1 = 1.0

                price_cake_usd = 0.0
                if pending_cake > 0.0 and pending_cake_usd_est > 0.0:
                    price_cake_usd = pending_cake_usd_est / pending_cake

                # -------------------------
                # 6) APR (minuto = atual; segundo = ticks)
                # -------------------------
                total_position_usd = 0.0
                total_vault_idle_usd = 0.0
                totals_usd = 0.0
                if st:
                    holdings = st.get("holdings") or {}
                    in_position = holdings.get("in_position") or {}
                    total_position_usd = float(in_position.get("total_usd") or 0.0)
                    vault_idle = holdings.get("vault_idle") or {}
                    total_vault_idle_usd = float(vault_idle.get("total_usd") or 0.0)
                    totals = holdings.get("totals") or {}
                    totals_usd = float(totals.get("total_usd") or 0.0)

                qty_candles = int(last_episode.last_event_bar or 0)
                out_above_streak_total = int(last_episode.out_above_streak_total or 0)
                out_below_streak_total = int(last_episode.out_below_streak_total or 0)
                total_candle_out = out_above_streak_total + out_below_streak_total

                qty_candles_in_formula = float(qty_candles - total_candle_out)
                if qty_candles_in_formula <= 0.0:
                    qty_candles_in_formula = 1.0

                APR_daily = 0.0
                APR_annualy = 0.0
                percentage_fee_vs_position = 0.0

                fees_this_episode_usd = float(fees_uncollected_st_usd + gauge_rewards_st_usd)

                # ---- APR atual (por minuto) usando candles ----
                if total_position_usd > 0.0 and fees_this_episode_usd > 0.0:
                    percentage_fee_vs_position = fees_this_episode_usd / total_position_usd
                    APR_daily = (1440.0 / qty_candles_in_formula) * percentage_fee_vs_position
                    APR_annualy = APR_daily * 365.0

                APR_daily_pct = APR_daily * 100.0
                APR_annualy_pct = APR_annualy * 100.0

                # ---- APR por segundo (ticks) + APR por minuto (ticks) ----
                apr_ticks_annual_by_min_pct = 0.0
                apr_ticks_annual_by_sec_pct = 0.0
                ticks_seconds_in_range = 0.0
                ticks_minutes_in_range = 0.0
                ticks_count = 0
                tick_calc_used = False
                tick_calc_reason = None

                prev_open_ts = (
                    last_episode.open_time_iso
                    or last_episode.created_at_iso
                )
                prev_close_ts = (
                    last_episode.close_time_iso
                    or last_episode.close_time
                )

                prev_lower = last_episode.Pa
                prev_upper = last_episode.Pb

                ts_from_ms = self._parse_ts_ms(prev_open_ts)
                ts_to_ms = self._parse_ts_ms(prev_close_ts)

                stream_key = (
                    last_episode.stream_key
                    or episode.stream_key
                )

                # ticks.price costuma ser p_t1_t0 (token1 per token0).
                # Quando token0 é USD e token1 é risco, a escala humana do range é USD por token1 (= p_t0_t1),
                # então invertemos o tick para comparar.
                invert_tick_price = bool(token0_is_usd and (not token1_is_usd))

                if (
                    stream_key
                    and ts_from_ms is not None
                    and ts_to_ms is not None
                    and ts_to_ms > ts_from_ms
                    and prev_lower is not None
                    and prev_upper is not None
                    and fees_this_episode_usd > 0.0
                    and total_position_usd > 0.0
                ):
                    try:
                        Pa_h = float(prev_lower)
                        Pb_h = float(prev_upper)

                        raw = await self._fetch_ticks_paginated(
                            stream_key=str(stream_key),
                            ts_from=int(ts_from_ms),
                            ts_to=int(ts_to_ms),
                            hard_limit=200_000,
                            page_limit=50_000,
                        )

                        ticks_count = int(len(raw))
                        ticks_pairs: List[Tuple[int, float]] = []
                        for r in raw:
                            try:
                                ts_i = int(r.get("ts"))
                                p_i = float(r.get("price"))
                                ticks_pairs.append((ts_i, p_i))
                            except Exception:
                                continue

                        ticks_pairs.sort(key=lambda x: x[0])

                        ticks_seconds_in_range = self._seconds_in_range_from_ticks(
                            ticks=ticks_pairs,
                            Pa=Pa_h,
                            Pb=Pb_h,
                            invert_price=invert_tick_price,
                        )
                        ticks_minutes_in_range = float(ticks_seconds_in_range / 60.0)

                        apr_ticks_annual_by_min_pct = self._apr_annual_pct_from_minutes(
                            fees_usd=fees_this_episode_usd,
                            base_usd=total_position_usd,
                            minutes_in_range=ticks_minutes_in_range,
                        )
                        apr_ticks_annual_by_sec_pct = self._apr_annual_pct_from_seconds(
                            fees_usd=fees_this_episode_usd,
                            base_usd=total_position_usd,
                            seconds_in_range=ticks_seconds_in_range,
                        )

                        tick_calc_used = True
                    except Exception as exc:
                        tick_calc_used = False
                        tick_calc_reason = str(exc)
                else:
                    tick_calc_used = False
                    tick_calc_reason = "missing_inputs"

                # -------------------------
                # 7) Metadados de episódio
                # -------------------------
                cur_open_ts = (
                    episode.open_time_iso
                    or episode.created_at_iso
                )

                cur_lower = episode.Pa
                cur_upper = episode.Pb

                prev_labels = {
                    "pool_type": last_episode.pool_type,
                    "mode_on_open": last_episode.mode_on_open,
                    "majority_on_open": last_episode.majority_on_open,
                    "target_major_pct": last_episode.target_major_pct,
                    "target_minor_pct": last_episode.target_minor_pct,
                    "open_price_exec": (last_episode.open_price_exec or 0.0)
                }

                cur_labels = {
                    "pool_type": episode.pool_type,
                    "mode_on_open": episode.mode_on_open,
                    "majority_on_open": episode.majority_on_open,
                    "target_major_pct": episode.target_major_pct,
                    "target_minor_pct": episode.target_minor_pct,
                    "open_price_exec": (episode.open_price_exec or 0.0)
                }

                episode_meta = {
                    "dex": dex,
                    "alias": alias,
                    "episode_id": episode_id,
                    "last_episode_id": last_episode_id,
                    "prev_open_ts": prev_open_ts,
                    "prev_close_ts": prev_close_ts,
                    "cur_open_ts": cur_open_ts,
                    "prev_range": {"Pa": prev_lower, "Pb": prev_upper},
                    "cur_range": {"Pa": cur_lower, "Pb": cur_upper},
                    "prev_labels": prev_labels,
                    "cur_labels": cur_labels,
                }

                # -------------------------
                # 8) Monta métricas completas
                # -------------------------
                metrics = {
                    "episode_meta": episode_meta,

                    "fees_uncollected_st_usd": fees_uncollected_st_usd,
                    "gauge_rewards_st_usd": gauge_rewards_st_usd,
                    "fees_this_episode_usd": fees_this_episode_usd,
                    "price_cake_usd_ref": price_cake_usd,
                    "total_position_usd": total_position_usd,
                    "total_vault_idle_usd": total_vault_idle_usd,
                    "totals_usd": totals_usd,

                    # APR antigo (por minuto / candles)
                    "qty_candles": qty_candles,
                    "total_candle_out": total_candle_out,
                    "qty_candles_in_formula": qty_candles_in_formula,
                    "percentage_fee_vs_position": percentage_fee_vs_position,
                    "APR_daily": APR_daily,
                    "APR_annualy": APR_annualy,
                    "APR_daily_pct": APR_daily_pct,
                    "APR_annualy_pct": APR_annualy_pct,

                    # APR ticks (por minuto e por segundo)
                    "tick_apr_used": bool(tick_calc_used),
                    "tick_apr_reason": tick_calc_reason,
                    "tick_stream_key": stream_key,
                    "tick_ts_from_ms": ts_from_ms,
                    "tick_ts_to_ms": ts_to_ms,
                    "tick_count": ticks_count,
                    "tick_seconds_in_range": float(ticks_seconds_in_range),
                    "tick_minutes_in_range": float(ticks_minutes_in_range),

                    "APR_annualy_ticks_by_min_pct": float(apr_ticks_annual_by_min_pct),
                    "APR_annualy_ticks_by_sec_pct": float(apr_ticks_annual_by_sec_pct),
                }

                metrics_safe = sanitize_for_bson(metrics)

                await self._episode_repo.update_partial(
                    last_episode_id,
                    {
                        "metrics": metrics_safe
                    },
                )

                # -------------------------
                # 9) Notificação Telegram
                # -------------------------
                if getattr(self, "_notifier", None) is not None:
                    lines: List[str] = []

                    lines.append("")
                    lines.append("")
                    lines.append("**LP episode fechado e nova posição aberta**")
                    lines.append(f"Dex/Alias: {dex}/{alias}")
                    lines.append(f"Open Episódio anterior: {prev_labels.get('open_price_exec')}")
                    lines.append(f"Open Episódio atual: {cur_labels.get('open_price_exec')}")
                    lines.append("")

                    lines.append("**📌 Episódio anterior (pool fechada)**")
                    lines.append(f"Abertura: {prev_open_ts}")
                    lines.append(f"Fechamento: {prev_close_ts}")
                    lines.append(f"Range preços: Pa={prev_lower}, Pb={prev_upper}")
                    lines.append("**Configuração da pool:**")
                    lines.append(f"• Tipo de pool: {prev_labels.get('pool_type')}")
                    lines.append(f"• Direção (tendência): {prev_labels.get('mode_on_open')}")
                    lines.append("")

                    lines.append("**📌 Novo episódio (pool aberta)**")
                    lines.append(f"Abertura: {cur_open_ts}")
                    lines.append(f"Range preços: Pa={cur_lower}, Pb={cur_upper}")
                    lines.append("**Configuração da pool:**")
                    lines.append(f"• Tipo de pool: {cur_labels.get('pool_type')}")
                    lines.append(f"• Direção (tendência): {cur_labels.get('mode_on_open')}")
                    lines.append("")

                    lines.append("**📈 Métricas – Episódio encerrado**")
                    lines.append(f"• Fees LP uncollected: {fees_uncollected_st_usd:.8f}")
                    lines.append(f"• Rewards em USDC: {gauge_rewards_st_usd:.8f}")
                    lines.append(f"• Total fees do episódio (USD): {fees_this_episode_usd:.6f}")

                    # APR minuto/candles
                    lines.append(f"• APR diário aproximado (candles): {APR_daily_pct:.4f}%")
                    lines.append(f"• APR anualizado aproximado (candles): {APR_annualy_pct:.4f}%")
                    lines.append("")

                    # APR ticks
                    lines.append("**⏱️ APR por tempo em-range (ticks 5s)**")
                    if tick_calc_used:
                        lines.append(f"• Tempo in-range (seg): {ticks_seconds_in_range:.2f}")
                        lines.append(f"• Tempo in-range (min): {ticks_minutes_in_range:.4f}")
                        lines.append(f"• APR anualizado (ticks / minuto): {apr_ticks_annual_by_min_pct:.4f}%")
                        lines.append(f"• APR anualizado (ticks / segundo): {apr_ticks_annual_by_sec_pct:.4f}%")
                    else:
                        lines.append(f"• Tick APR indisponível: {tick_calc_reason}")
                    lines.append("")

                    lines.append("**🧮 Painel APR (inputs)**")
                    lines.append(f"• Posição USD no fechamento: {total_position_usd:.6f}")
                    lines.append(f"• Nº total de candles: {qty_candles}")
                    lines.append(f"• Candles fora da pool: {total_candle_out}")
                    lines.append(f"• Candles válidos p/ APR: {qty_candles_in_formula:.2f}")
                    lines.append(f"• Percentual fees/posição: {(percentage_fee_vs_position * 100):.4f}%")
                    lines.append("")

                    text = "\n".join(lines)
                    await self._notifier.send_message(text)

        except Exception as exc:
            self._logger.warning("Falha ao calcular métricas ou enviar Telegram: %s", exc)

        return True