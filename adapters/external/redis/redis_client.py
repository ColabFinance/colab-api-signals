from __future__ import annotations

import redis.asyncio as redis
from redis.asyncio import Redis

from config.settings import settings


def get_redis_client(*, socket_timeout_s: float = 30.0) -> Redis:
    """
    Build an async Redis client for api-signals.

    The socket timeout must stay comfortably above any blocking Redis command
    timeout used by the consumer worker, such as XREADGROUP BLOCK.
    """
    return redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=float(socket_timeout_s),
        retry_on_timeout=True,
    )