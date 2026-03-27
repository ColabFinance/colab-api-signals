from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from adapters.entry.http.strategy_episode_router import router as episodes_router
from adapters.entry.http.strategy_router import router as strategy_params_router
from adapters.entry.http.trade_strategy_router import router as trade_strategy_router
from adapters.entry.http.trigger_router import router as triggers_router
from adapters.external.database.mongodb_client import get_mongo_client
from config.settings import settings
from workers.signal_executor_supervisor import SignalExecutorSupervisor


def _setup_logging() -> None:
    """
    Configure application logging.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    """
    _setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting api-signals (lifespan startup)...")

    mongo_client = get_mongo_client()
    db = mongo_client[settings.MONGODB_DB_NAME]

    app.state.mongo_client = mongo_client
    app.state.mongo_db = db

    app.state.market_data_base_url = settings.MARKET_DATA_BASE_URL
    app.state.pipeline_base_url = settings.LP_BASE_URL

    try:
        await db.command("ping")
        logger.info("MongoDB ping ok.")
    except Exception:
        logger.exception("MongoDB ping failed (startup).")
        raise

    poll_interval_s = float(os.getenv("SIGNAL_EXECUTOR_POLL_INTERVAL_S", "2"))
    app.state.trigger_max_concurrency = int(os.getenv("TRIGGER_MAX_CONCURRENCY", "10"))

    executor = SignalExecutorSupervisor(
        mongo_client=mongo_client,
        db=db,
        lp_base_url=settings.LP_BASE_URL,
        trade_execution_base_url=settings.TRADE_EXECUTION_BASE_URL,
        telegram_bot_token=settings.TELEGRAM_BOT_TOKEN,
        telegram_chat_id=settings.TELEGRAM_CHAT_ID,
        poll_interval_s=poll_interval_s,
    )
    app.state.signal_executor = executor

    await executor.start()

    try:
        yield
    finally:
        logger.info("Shutting down api-signals (lifespan shutdown)...")

        try:
            if getattr(app.state, "signal_executor", None) is not None:
                await app.state.signal_executor.stop()
        except Exception:
            logger.exception("Failed stopping SignalExecutorSupervisor.")

        mongo_client.close()
        logger.info("MongoDB client closed.")


app = FastAPI(title="api-signals", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(strategy_params_router, prefix="/api")
app.include_router(triggers_router, prefix="/api")
app.include_router(episodes_router, prefix="/api")
app.include_router(trade_strategy_router, prefix="/api")


@app.get("/healthz")
async def healthz():
    """
    Basic health endpoint.
    """
    return {"status": "ok"}