import asyncpg
from typing import AsyncGenerator
from contextlib import asynccontextmanager

@asynccontextmanager
async def get_pg_connection() -> AsyncGenerator[None, asyncpg.Connection]:

    connection: asyncpg.Connection = await asyncpg.connect(
        user='blausher',
        password='postgres',
        database='back',
        host='localhost',
        port=5432
    )

    yield connection 

    await connection.close() 