import pytest

from app.clients import postgres


class DummyAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyPool:
    def __init__(self):
        self.connection = object()
        self.acquire_calls = 0
        self.closed = False

    def acquire(self):
        self.acquire_calls += 1
        return DummyAcquire(self.connection)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_init_pg_pool_reuses_existing_pool(monkeypatch):
    created_pools = []

    async def create_pool_stub(**kwargs):
        pool = DummyPool()
        created_pools.append((pool, kwargs))
        return pool

    await postgres.close_pg_pool()
    monkeypatch.setattr(postgres.asyncpg, "create_pool", create_pool_stub)

    first_pool = await postgres.init_pg_pool()
    second_pool = await postgres.init_pg_pool()

    assert first_pool is second_pool
    assert len(created_pools) == 1
    assert created_pools[0][1]["database"] == "back"

    await postgres.close_pg_pool()


@pytest.mark.asyncio
async def test_get_pg_connection_acquires_from_pool(monkeypatch):
    pool = DummyPool()

    async def init_pool_stub():
        return pool

    monkeypatch.setattr(postgres, "init_pg_pool", init_pool_stub)

    async with postgres.get_pg_connection() as connection:
        assert connection is pool.connection

    assert pool.acquire_calls == 1


@pytest.mark.asyncio
async def test_close_pg_pool_discards_pool_from_other_event_loop():
    pool = DummyPool()
    postgres._pg_pool = pool
    postgres._pg_pool_loop = object()

    await postgres.close_pg_pool()

    assert pool.closed is False
    assert postgres._pg_pool is None
    assert postgres._pg_pool_loop is None


@pytest.mark.asyncio
async def test_init_pg_pool_recreates_pool_for_new_event_loop(monkeypatch):
    created_pools = []
    first_loop = object()
    second_loop = object()
    loops = iter([first_loop, first_loop, second_loop, second_loop])

    async def create_pool_stub(**kwargs):
        pool = DummyPool()
        created_pools.append((pool, kwargs))
        return pool

    await postgres.close_pg_pool()
    monkeypatch.setattr(postgres.asyncpg, "create_pool", create_pool_stub)
    monkeypatch.setattr(postgres.asyncio, "get_running_loop", lambda: next(loops))

    first_pool = await postgres.init_pg_pool()
    second_pool = await postgres.init_pg_pool()
    third_pool = await postgres.init_pg_pool()

    assert first_pool is second_pool
    assert third_pool is not first_pool
    assert first_pool.closed is False
    assert len(created_pools) == 2

    await postgres.close_pg_pool()


@pytest.mark.asyncio
async def test_close_pg_pool_closes_pool_from_current_event_loop():
    pool = DummyPool()
    current_loop = postgres.asyncio.get_running_loop()
    postgres._pg_pool = pool
    postgres._pg_pool_loop = current_loop

    await postgres.close_pg_pool()

    assert pool.closed is True
    assert postgres._pg_pool is None
    assert postgres._pg_pool_loop is None
