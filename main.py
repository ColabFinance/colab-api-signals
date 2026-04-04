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
from adapters.external.database.trade_strategy_runtime_event_repository_mongodb import (
    TradeStrategyRuntimeEventRepositoryMongoDB,
)
from adapters.external.database.trade_strategy_runtime_snapshot_repository_mongodb import (
    TradeStrategyRuntimeSnapshotRepositoryMongoDB,
)
from adapters.external.market_data.market_data_http_client import MarketDataHttpClient
from adapters.external.notify.telegram_notifier import TelegramNotifier
from adapters.external.redis.redis_client import get_redis_client
from adapters.external.redis.trade_candle_buffer_repository_redis import (
    TradeCandleBufferRepositoryRedis,
)
from adapters.external.redis.trade_pipeline_stream_publisher import TradePipelineStreamPublisher
from adapters.external.redis.trade_strategy_cache_repository_redis import (
    TradeStrategyCacheRepositoryRedis,
)
from adapters.external.trade_execution.trade_execution_http_client import (
    TradeExecutionHttpClient,
)
from config.settings import settings
from core.usecases.execute_trade_signal_pipeline_use_case import (
    ExecuteTradeSignalPipelineUseCase,
)
from core.usecases.process_trade_candle_closed_event_use_case import (
    ProcessTradeCandleClosedEventUseCase,
)
from core.usecases.trade_candle_buffer_use_case import TradeCandleBufferUseCase
from workers.signal_executor_supervisor import SignalExecutorSupervisor
from workers.trade_candle_evaluation_shard_worker import TradeCandleEvaluationShardWorker
from workers.trade_candle_ingress_dispatcher_worker import TradeCandleIngressDispatcherWorker
from workers.trade_signal_stream_consumer_worker import TradeSignalStreamConsumerWorker


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
        float(
            max(
                settings.REDIS_TRADE_CANDLE_STREAM_BLOCK_MS,
                settings.REDIS_TRADE_EVAL_BLOCK_MS,
                settings.REDIS_TRADE_SIGNAL_BLOCK_MS,
            )
        ) / 1000.0 + 10.0,
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

    # LP executor remains separated from trade execution.
    lp_executor = SignalExecutorSupervisor(
        mongo_client=mongo_client,
        db=db,
        lp_base_url=settings.LP_BASE_URL,
        telegram_bot_token=settings.TELEGRAM_BOT_TOKEN,
        telegram_chat_id=settings.TELEGRAM_CHAT_ID,
        poll_interval_s=poll_interval_s,
    )
    app.state.signal_executor = lp_executor

    market_data_client = MarketDataHttpClient(base_url=settings.MARKET_DATA_BASE_URL)
    app.state.market_data_client = market_data_client

    trade_strategy_repo = TradeStrategyRepositoryMongoDB(db)
    trade_signal_repo = TradeSignalRepositoryMongoDB(db)
    trade_runtime_snapshot_repo = TradeStrategyRuntimeSnapshotRepositoryMongoDB(db)
    trade_runtime_event_repo = TradeStrategyRuntimeEventRepositoryMongoDB(db)

    # Ensure trade indexes used by evaluation/execution.
    await trade_strategy_repo.ensure_indexes()
    await trade_signal_repo.ensure_indexes()
    await trade_runtime_snapshot_repo.ensure_indexes()
    await trade_runtime_event_repo.ensure_indexes()

    trade_candle_buffer_repo = TradeCandleBufferRepositoryRedis(
        redis_client=redis_client,
        key_prefix=settings.TRADE_CANDLE_BUFFER_KEY_PREFIX,
        maxlen=settings.TRADE_CANDLE_BUFFER_MAXLEN,
    )
    app.state.trade_candle_buffer_repo = trade_candle_buffer_repo

    trade_strategy_cache_repo = TradeStrategyCacheRepositoryRedis(
        redis_client=redis_client,
        key_prefix=settings.TRADE_STRATEGY_CACHE_KEY_PREFIX,
        ttl_s=settings.TRADE_STRATEGY_CACHE_TTL_S,
    )
    app.state.trade_strategy_cache_repo = trade_strategy_cache_repo

    trade_candle_buffer_uc = TradeCandleBufferUseCase(
        candle_buffer_repo=trade_candle_buffer_repo,
        market_data_client=market_data_client,
    )
    app.state.trade_candle_buffer_uc = trade_candle_buffer_uc

    trade_pipeline_publisher = TradePipelineStreamPublisher(
        redis_client=redis_client,
        eval_stream_prefix=settings.REDIS_TRADE_EVAL_STREAM_PREFIX,
        eval_shard_count=settings.REDIS_TRADE_EVAL_SHARD_COUNT,
        signal_stream_name=settings.REDIS_TRADE_SIGNAL_STREAM,
        stream_maxlen=settings.REDIS_PIPELINE_STREAM_MAXLEN,
        logger=logging.getLogger("TradePipelinePublisher"),
    )
    app.state.trade_pipeline_publisher = trade_pipeline_publisher

    trade_candle_processor = ProcessTradeCandleClosedEventUseCase(
        strategy_repo=trade_strategy_repo,
        signal_repo=trade_signal_repo,
        runtime_snapshot_repo=trade_runtime_snapshot_repo,
        runtime_event_repo=trade_runtime_event_repo,
        candle_buffer_repo=trade_candle_buffer_repo,
        market_data_client=market_data_client,
        strategy_cache_repo=trade_strategy_cache_repo,
        logger=logging.getLogger("TradeCandleProcessor"),
        buffer_maxlen=settings.TRADE_CANDLE_BUFFER_MAXLEN,
    )
    app.state.trade_candle_processor = trade_candle_processor

    ingress_dispatcher = TradeCandleIngressDispatcherWorker(
        redis_client=redis_client,
        publisher=trade_pipeline_publisher,
        source_stream_name=settings.REDIS_TRADE_CANDLE_STREAM,
        group_name=settings.REDIS_TRADE_CANDLE_INGEST_GROUP,
        consumer_name=settings.REDIS_TRADE_CANDLE_INGEST_CONSUMER_NAME,
        block_ms=settings.REDIS_TRADE_CANDLE_STREAM_BLOCK_MS,
        read_count=settings.REDIS_TRADE_CANDLE_STREAM_READ_COUNT,
        logger=logging.getLogger("TradeCandleIngressDispatcher"),
    )
    app.state.trade_candle_ingress_dispatcher = ingress_dispatcher

    eval_workers: list[TradeCandleEvaluationShardWorker] = []
    for shard_id in range(settings.REDIS_TRADE_EVAL_SHARD_COUNT):
        stream_name = trade_pipeline_publisher.build_eval_stream_name(shard_id)
        group_name = f"{settings.REDIS_TRADE_EVAL_GROUP_PREFIX}-{shard_id}"
        consumer_name = f"{settings.REDIS_TRADE_EVAL_CONSUMER_PREFIX}-{shard_id}"

        worker = TradeCandleEvaluationShardWorker(
            redis_client=redis_client,
            processor=trade_candle_processor,
            publisher=trade_pipeline_publisher,
            shard_id=shard_id,
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
            block_ms=settings.REDIS_TRADE_EVAL_BLOCK_MS,
            read_count=settings.REDIS_TRADE_EVAL_READ_COUNT,
            logger=logging.getLogger(f"TradeCandleEvaluationShard[{shard_id}]"),
        )
        eval_workers.append(worker)

    app.state.trade_candle_eval_workers = eval_workers

    notifier = None
    if settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID:
        notifier = TelegramNotifier(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=settings.TELEGRAM_CHAT_ID,
        )
        await notifier.start()

    app.state.telegram_notifier = notifier
    trade_execution_client = TradeExecutionHttpClient(base_url=settings.TRADE_EXECUTION_BASE_URL)
    app.state.trade_execution_client = trade_execution_client

    trade_signal_execution_uc = ExecuteTradeSignalPipelineUseCase(
        trade_signal_repo=trade_signal_repo,
        runtime_snapshot_repo=trade_runtime_snapshot_repo,
        trade_execution_client=trade_execution_client,
        market_data_client=market_data_client,
        notifier=notifier,
    )
    app.state.trade_signal_execution_uc = trade_signal_execution_uc

    trade_signal_execution_worker = TradeSignalStreamConsumerWorker(
        redis_client=redis_client,
        executor_use_case=trade_signal_execution_uc,
        stream_name=settings.REDIS_TRADE_SIGNAL_STREAM,
        group_name=settings.REDIS_TRADE_SIGNAL_GROUP,
        consumer_name=settings.REDIS_TRADE_SIGNAL_CONSUMER_NAME,
        block_ms=settings.REDIS_TRADE_SIGNAL_BLOCK_MS,
        read_count=settings.REDIS_TRADE_SIGNAL_READ_COUNT,
        logger=logging.getLogger("TradeSignalExecutionConsumer"),
    )
    app.state.trade_signal_execution_worker = trade_signal_execution_worker

    await lp_executor.start()
    await ingress_dispatcher.start()

    for worker in eval_workers:
        await worker.start()

    await trade_signal_execution_worker.start()

    try:
        yield
    finally:
        logger.info("Shutting down api-signals (lifespan shutdown)...")

        try:
            if getattr(app.state, "trade_signal_execution_worker", None) is not None:
                await app.state.trade_signal_execution_worker.stop()
        except Exception:
            logger.exception("Failed stopping TradeSignalStreamConsumerWorker.")

        try:
            for worker in getattr(app.state, "trade_candle_eval_workers", []) or []:
                await worker.stop()
        except Exception:
            logger.exception("Failed stopping TradeCandleEvaluationShardWorker list.")

        try:
            if getattr(app.state, "trade_candle_ingress_dispatcher", None) is not None:
                await app.state.trade_candle_ingress_dispatcher.stop()
        except Exception:
            logger.exception("Failed stopping TradeCandleIngressDispatcherWorker.")

        try:
            if getattr(app.state, "signal_executor", None) is not None:
                await app.state.signal_executor.stop()
        except Exception:
            logger.exception("Failed stopping SignalExecutorSupervisor.")

        try:
            if getattr(app.state, "trade_execution_client", None) is not None:
                await app.state.trade_execution_client.aclose()
        except Exception:
            logger.exception("Failed closing TradeExecutionHttpClient.")

        try:
            if getattr(app.state, "market_data_client", None) is not None:
                await app.state.market_data_client.aclose()
        except Exception:
            logger.exception("Failed closing MarketDataHttpClient.")

        try:
            if getattr(app.state, "redis_client", None) is not None:
                await app.state.redis_client.aclose()
                logger.info("Redis client closed.")
        except Exception:
            logger.exception("Failed closing Redis client.")

        try:
            if getattr(app.state, "telegram_notifier", None) is not None:
                await app.state.telegram_notifier.stop()
        except Exception:
            logger.exception("Failed closing TelegramNotifier.")
            
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