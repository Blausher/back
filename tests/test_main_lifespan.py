import pytest

from app import main


@pytest.mark.asyncio
async def test_lifespan_initializes_and_closes_shared_clients(monkeypatch):
    calls = []

    async def init_pg_pool_stub():
        calls.append("pg_start")

    async def close_pg_pool_stub():
        calls.append("pg_stop")

    async def kafka_start_stub():
        calls.append("kafka_start")

    async def kafka_stop_stub():
        calls.append("kafka_stop")

    monkeypatch.setattr(main, "init_pg_pool", init_pg_pool_stub)
    monkeypatch.setattr(main, "close_pg_pool", close_pg_pool_stub)
    monkeypatch.setattr(main.kafka_client, "start", kafka_start_stub)
    monkeypatch.setattr(main.kafka_client, "stop", kafka_stop_stub)

    async with main.lifespan(main.app):
        calls.append("inside")

    assert calls == ["pg_start", "kafka_start", "inside", "kafka_stop", "pg_stop"]
