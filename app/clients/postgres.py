import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg


_pg_pool: asyncpg.Pool | None = None


async def init_pg_pool() -> asyncpg.Pool:
    global _pg_pool

    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(
            user=os.getenv('POSTGRES_USER', 'blausher'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'back'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
        )

    return _pg_pool


async def close_pg_pool() -> None:
    global _pg_pool

    if _pg_pool is not None:
        await _pg_pool.close()
        _pg_pool = None


@asynccontextmanager
async def get_pg_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await init_pg_pool()

    async with pool.acquire() as connection:
        yield connection
