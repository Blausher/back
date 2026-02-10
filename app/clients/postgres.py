import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg

@asynccontextmanager
async def get_pg_connection() -> AsyncGenerator[None, asyncpg.Connection]:

    connection: asyncpg.Connection = await asyncpg.connect(
        user=os.getenv('POSTGRES_USER', 'blausher'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
        database=os.getenv('POSTGRES_DB', 'back'),
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
    )

    yield connection 

    await connection.close() 
