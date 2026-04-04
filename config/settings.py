from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """
    Application configuration for api-signals.
    """

    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "signals_db")
    MONGODB_MAX_POOL_SIZE: int = int(os.getenv("MONGODB_MAX_POOL_SIZE", "50"))
    MONGODB_SERVER_SELECTION_TIMEOUT_MS: int = int(
        os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "5000")
    )
    MONGODB_CONNECT_TIMEOUT_MS: int = int(os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "3000"))
    MONGODB_SOCKET_TIMEOUT_MS: int = int(os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "10000"))

    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    MARKET_DATA_BASE_URL: str = os.getenv("MARKET_DATA_BASE_URL", "http://172.17.0.1:8081")
    LP_BASE_URL: str = os.getenv("LP_BASE_URL", "http://172.17.0.1:8000")
    TRADE_EXECUTION_BASE_URL: str = os.getenv("TRADE_EXECUTION_BASE_URL", "http://172.17.0.1:8002")

    REDIS_URL: str = os.getenv("REDIS_URL", "redis://host.docker.internal:6379/0")
    REDIS_TRADE_CANDLE_STREAM: str = os.getenv("REDIS_TRADE_CANDLE_STREAM", "trade.candle.closed.v1")
    REDIS_TRADE_CANDLE_GROUP: str = os.getenv("REDIS_TRADE_CANDLE_GROUP", "api-signals-trade")
    REDIS_TRADE_CANDLE_CONSUMER_NAME: str = os.getenv(
        "REDIS_TRADE_CANDLE_CONSUMER_NAME",
        f"{os.getenv('APP_NAME', 'api-signals')}-trade",
    )
    TRADE_CANDLE_STREAM_BLOCK_MS: int = int(os.getenv("TRADE_CANDLE_STREAM_BLOCK_MS", "5000"))
    TRADE_CANDLE_STREAM_READ_COUNT: int = int(os.getenv("TRADE_CANDLE_STREAM_READ_COUNT", "1"))
    TRADE_CANDLE_BUFFER_KEY_PREFIX: str = os.getenv("TRADE_CANDLE_BUFFER_KEY_PREFIX", "trade:candles")
    TRADE_CANDLE_BUFFER_MAXLEN: int = int(os.getenv("TRADE_CANDLE_BUFFER_MAXLEN", "2000"))

    ENABLE_BACKFILL_ON_START: bool = os.getenv("ENABLE_BACKFILL_ON_START", "true").lower() == "true"

    APP_NAME: str = os.getenv("APP_NAME", "api-signals")

    PRIVY_APP_ID: str = os.getenv("PRIVY_APP_ID", "")
    PRIVY_JWKS_URL: str = os.getenv("PRIVY_JWKS_URL", "https://auth.privy.io/api/v1/apps/jwks")
    ADMIN_WALLETS: str = os.getenv("ADMIN_WALLETS", "")
    PRIVY_APP_SECRET: str = os.getenv("PRIVY_APP_SECRET", "")


settings = Settings()