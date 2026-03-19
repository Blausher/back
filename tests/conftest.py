import pytest_asyncio

from app.clients.postgres import close_pg_pool


@pytest_asyncio.fixture(autouse=True)
async def close_shared_postgres_pool_between_async_tests():
    yield
    await close_pg_pool()
