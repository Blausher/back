import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as redis


@asynccontextmanager
async def get_redis_connection() -> AsyncGenerator[redis.Redis, None]:
    connection = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
    )

    yield connection

    await connection.aclose()
