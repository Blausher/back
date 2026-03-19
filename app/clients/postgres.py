import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg


_pg_pool: asyncpg.Pool | None = None
_pg_pool_loop: asyncio.AbstractEventLoop | None = None


def _discard_pool() -> None:
    global _pg_pool, _pg_pool_loop

    _pg_pool = None
    _pg_pool_loop = None


async def init_pg_pool() -> asyncpg.Pool:
    global _pg_pool, _pg_pool_loop

    current_loop = asyncio.get_running_loop()

    if _pg_pool is not None and _pg_pool_loop is not current_loop:
        _discard_pool()

    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(
            user=os.getenv('POSTGRES_USER', 'blausher'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'back'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
        )
        _pg_pool_loop = current_loop

    return _pg_pool


async def close_pg_pool() -> None:
    global _pg_pool, _pg_pool_loop

    if _pg_pool is not None:
        current_loop = asyncio.get_running_loop()
        if _pg_pool_loop is current_loop:
            await _pg_pool.close()
        else:
            _discard_pool()
            return
        _pg_pool = None
        _pg_pool_loop = None


@asynccontextmanager
async def get_pg_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await init_pg_pool()

    async with pool.acquire() as connection:
        yield connection
