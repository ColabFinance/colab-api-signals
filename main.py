from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from adapters.entry.http.strategy_episode_router import router as episodes_router
from adapters.entry.http.strategy_router import router as strategy_params_router
from adapters.entry.http.trade_runtime_router import router as trade_runtime_router
from adapters.entry.http.trade_strategy_router import router as trade_strategy_router
from adapters.entry.http.trigger_router import router as triggers_router
from adapters.external.database.mongodb_client import get_mongo_client
from adapters.external.database.trade_signal_repository_mongodb import TradeSignalRepositoryMongoDB
from adapters.external.database.trade_strategy_repository_mongodb import TradeStrategyRepositoryMongoDB
from adapters.external.database.trade_strategy_runtime_snapshot_repository_mongodb import (
    TradeStrategyRuntimeSnapshotRepositoryMongoDB,
)
from adapters.external.database.trade_trigger_event_repository_mongodb import (
    TradeTriggerEventRepositoryMongoDB,
)
from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.redis.redis_client import get_redis_client
from adapters.external.redis.trade_candle_buffer_repository_redis import (
    TradeCandleBufferRepositoryRedis,
)
from config.settings import settings
from core.usecases.process_trade_candle_closed_event_use_case import (
    ProcessTradeCandleClosedEventUseCase,
)
from core.usecases.trade_candle_buffer_use_case import TradeCandleBufferUseCase
from workers.signal_executor_supervisor import SignalExecutorSupervisor
from workers.trade_candle_stream_consumer import TradeCandleStreamConsumerWorker


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

    redis_socket_timeout_s = max(
        30.0,
        float(settings.TRADE_CANDLE_STREAM_BLOCK_MS) / 1000.0 + 10.0,
    )

    redis_client = get_redis_client(socket_timeout_s=redis_socket_timeout_s)
    try:
        await redis_client.ping()
        logger.info("Redis ping ok.")
    except Exception:
        logger.exception("Redis ping failed (startup).")
        raise

    app.state.redis_client = redis_client

    poll_interval_s = float(os.getenv("SIGNAL_EXECUTOR_POLL_INTERVAL_S", "2"))

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

    await TradeTriggerEventRepositoryMongoDB(db).ensure_indexes()

    market_data_client = MarketDataHttpClient(base_url=settings.MARKET_DATA_BASE_URL)

    trade_candle_buffer_repo = TradeCandleBufferRepositoryRedis(
        redis_client=redis_client,
        key_prefix=settings.TRADE_CANDLE_BUFFER_KEY_PREFIX,
        maxlen=settings.TRADE_CANDLE_BUFFER_MAXLEN,
    )
    app.state.trade_candle_buffer_repo = trade_candle_buffer_repo

    trade_candle_buffer_uc = TradeCandleBufferUseCase(
        candle_buffer_repo=trade_candle_buffer_repo,
        market_data_client=market_data_client,
    )
    app.state.trade_candle_buffer_uc = trade_candle_buffer_uc

    trade_candle_processor = ProcessTradeCandleClosedEventUseCase(
        trigger_event_repo=TradeTriggerEventRepositoryMongoDB(db),
        strategy_repo=TradeStrategyRepositoryMongoDB(db),
        signal_repo=TradeSignalRepositoryMongoDB(db),
        runtime_snapshot_repo=TradeStrategyRuntimeSnapshotRepositoryMongoDB(db),
        candle_buffer_repo=trade_candle_buffer_repo,
        market_data_client=market_data_client,
        logger=logger,
        signal_waker=executor.wake,
        buffer_maxlen=settings.TRADE_CANDLE_BUFFER_MAXLEN,
    )
    app.state.trade_candle_processor = trade_candle_processor

    trade_candle_consumer = TradeCandleStreamConsumerWorker(
        redis_client=redis_client,
        processor=trade_candle_processor,
        stream_name=settings.REDIS_TRADE_CANDLE_STREAM,
        group_name=settings.REDIS_TRADE_CANDLE_GROUP,
        consumer_name=settings.REDIS_TRADE_CANDLE_CONSUMER_NAME,
        block_ms=settings.TRADE_CANDLE_STREAM_BLOCK_MS,
        read_count=settings.TRADE_CANDLE_STREAM_READ_COUNT,
        logger=logging.getLogger("TradeCandleStreamConsumer"),
    )
    app.state.trade_candle_consumer = trade_candle_consumer

    await executor.start()
    await trade_candle_consumer.start()

    try:
        yield
    finally:
        logger.info("Shutting down api-signals (lifespan shutdown)...")

        try:
            if getattr(app.state, "trade_candle_consumer", None) is not None:
                await app.state.trade_candle_consumer.stop()
        except Exception:
            logger.exception("Failed stopping TradeCandleStreamConsumerWorker.")

        try:
            if getattr(app.state, "signal_executor", None) is not None:
                await app.state.signal_executor.stop()
        except Exception:
            logger.exception("Failed stopping SignalExecutorSupervisor.")

        try:
            if getattr(app.state, "redis_client", None) is not None:
                await app.state.redis_client.aclose()
                logger.info("Redis client closed.")
        except Exception:
            logger.exception("Failed closing Redis client.")

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
app.include_router(trade_runtime_router, prefix="/api")


@app.get("/healthz")
async def healthz():
    """
    Basic health endpoint.
    """
    return {"status": "ok"}