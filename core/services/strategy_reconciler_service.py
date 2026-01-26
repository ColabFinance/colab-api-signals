from typing import Dict, Optional, List

from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity


class StrategyReconcilerService:
    """
    Compares desired episode state with on-chain LP state (via api-liquidity-provider)
    and produces an executable plan ("steps") for the pipeline.
    The steps array is strictly ordered. The pipeline MUST execute in order.

    Steps canonical order for a rotation is:
      1) COLLECT        (harvest fees to vault)
      2) WITHDRAW       (pull liquidity from pool back to vault idle balance)
      3) SWAP_EXACT_IN  (rebalance token proportions in idle balance)
      4) OPEN           (mint new range using idle balances)
    """

    def __init__(self, lp_client: PipelineHttpClient):
        self._lp = lp_client

    async def reconcile(self, strategy_id: str, desired: StrategyEpisodeEntity, symbol: str) -> Optional[Dict]:
        """
        Build an execution plan for the given desired episode.

        Returns:
            {
              "strategy_id": "...",
              "signal_type": "OPEN_NEW_RANGE" | "ROTATE_RANGE",
              "steps": [
                 {"action": "COLLECT", ...},
                 {"action": "WITHDRAW", ...},
                 {"action": "SWAP_EXACT_IN", ...},
                 {"action": "OPEN", ...}
              ],
              "episode": desired,
              "symbol": symbol
            }

        Or None if LP is already aligned.
        """
        dex = desired.dex
        alias = desired.alias

        gauge_flow = bool(desired.gauge_flow_enabled)
        
        # pull live vault status so we know if position exists / is aligned
        lp_status = None
        if dex and alias:
            lp_status = await self._lp.get_status(alias)

        # No LP or no position yet -> first time open
        if not lp_status or not lp_status.get("pool"):
            if dex and alias:
                steps = [
                    {"action": "OPEN", "payload": {"dex": dex, "alias": alias, "dynamic_range": True}}
                ]
                if gauge_flow:
                    steps.append({"action": "STAKE", "payload": {"dex": dex, "alias": alias}})
            else:
                steps = [
                    {"action": "NOOP_LEGACY", "payload": {"reason": "FIRST_OPEN_NO_VAULT"}}
                ]

            return {
                "strategy_id": strategy_id,
                "signal_type": "OPEN_NEW_RANGE",
                "steps": steps,
                "episode": desired,
                "symbol": symbol,
            }

        return self._build_full_plan(
            dex=dex,
            alias=alias,
            strategy_id=strategy_id,
            desired=desired,
            symbol=symbol,
            reason="DYNAMIC_RANGE_ROTATION",
        )

    def _build_full_plan(
        self,
        dex: Optional[str],
        alias: Optional[str],
        strategy_id: str,
        desired: StrategyEpisodeEntity,
        symbol: str,
        reason: str,
    ) -> Dict:
        steps: List[Dict] = []
        gauge_flow = bool(desired.gauge_flow_enabled)

        if dex and alias:
            if gauge_flow:
                steps.append({"action": "BATCH_REQUEST", "payload": {"dex": dex, "alias": alias, "dynamic_range": True}})
            else:
                steps.append({"action": "COLLECT", "payload": {"dex": dex, "alias": alias}})
                steps.append({"action": "WITHDRAW", "payload": {"dex": dex, "alias": alias, "mode": "pool"}})
                steps.append({"action": "SWAP_EXACT_IN", "payload": {"dex": dex, "alias": alias, "dynamic_range": True}})
                steps.append({"action": "OPEN", "payload": {"dex": dex, "alias": alias, "dynamic_range": True}})
        else:
            steps.append({"action": "NOOP_LEGACY", "payload": {"reason": reason}})

        return {
            "strategy_id": strategy_id,
            "signal_type": "ROTATE_RANGE",
            "steps": steps,
            "episode": desired,
            "symbol": symbol,
        }
