import json
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.clients.redis import get_redis_connection
from app.repositories import prediction_cache as cache_module
from app.repositories.prediction_cache import (
    ModerationResultRedisStorage,
    PredictionRedisStorage,
)


@pytest.mark.asyncio
async def test_prediction_storage_set_uses_pipeline_and_expected_args(monkeypatch):
    pipeline = MagicMock()
    pipeline.set = MagicMock(return_value=pipeline)
    pipeline.expire = MagicMock(return_value=pipeline)
    pipeline.execute = AsyncMock()

    connection = MagicMock()
    connection.pipeline = MagicMock(return_value=pipeline)

    @asynccontextmanager
    async def fake_redis_connection():
        yield connection

    monkeypatch.setattr(cache_module, "get_redis_connection", fake_redis_connection)

    storage = PredictionRedisStorage()
    payload = {"is_valid": True, "probability": 0.91}

    await storage.set(42, payload)

    connection.pipeline.assert_called_once_with()
    pipeline.set.assert_called_once()
    pipeline.expire.assert_called_once_with("prediction:42", 86400)
    pipeline.execute.assert_awaited_once()
    set_kwargs = pipeline.set.call_args.kwargs
    assert set_kwargs["name"] == "prediction:42"
    assert json.loads(set_kwargs["value"]) == payload


@pytest.mark.asyncio
async def test_prediction_storage_get_and_delete_use_expected_keys(monkeypatch):
    connection = MagicMock()
    connection.get = AsyncMock(return_value='{"is_valid": false, "probability": 0.12}')
    connection.delete = AsyncMock()

    @asynccontextmanager
    async def fake_redis_connection():
        yield connection

    monkeypatch.setattr(cache_module, "get_redis_connection", fake_redis_connection)

    storage = PredictionRedisStorage()

    row = await storage.get(17)
    await storage.delete(17)

    assert row == {"is_valid": False, "probability": 0.12}
    connection.get.assert_awaited_once_with("prediction:17")
    connection.delete.assert_awaited_once_with("prediction:17")


@pytest.mark.asyncio
async def test_moderation_storage_set_uses_pending_ttl(monkeypatch):
    pipeline = MagicMock()
    pipeline.set = MagicMock(return_value=pipeline)
    pipeline.expire = MagicMock(return_value=pipeline)
    pipeline.execute = AsyncMock()

    connection = MagicMock()
    connection.pipeline = MagicMock(return_value=pipeline)

    @asynccontextmanager
    async def fake_redis_connection():
        yield connection

    monkeypatch.setattr(cache_module, "get_redis_connection", fake_redis_connection)

    storage = ModerationResultRedisStorage()

    await storage.set(501, {"task_id": 501, "status": "pending", "probability": None, "is_violation": None})

    pipeline.expire.assert_called_once_with("moderation_result:501", 15)
    pipeline.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_moderation_storage_set_uses_terminal_ttl(monkeypatch):
    pipeline = MagicMock()
    pipeline.set = MagicMock(return_value=pipeline)
    pipeline.expire = MagicMock(return_value=pipeline)
    pipeline.execute = AsyncMock()

    connection = MagicMock()
    connection.pipeline = MagicMock(return_value=pipeline)

    @asynccontextmanager
    async def fake_redis_connection():
        yield connection

    monkeypatch.setattr(cache_module, "get_redis_connection", fake_redis_connection)

    storage = ModerationResultRedisStorage()

    await storage.set(502, {"task_id": 502, "status": "completed", "probability": 0.77, "is_violation": True})

    pipeline.expire.assert_called_once_with("moderation_result:502", 86400)
    pipeline.execute.assert_awaited_once()


async def _require_live_redis() -> None:
    try:
        async with get_redis_connection() as connection:
            await connection.ping()
    except Exception as exc:  # pragma: no cover - depends on runtime env
        pytest.skip(f"Redis is unavailable for integration test: {exc}")


@pytest.mark.asyncio
async def test_prediction_storage_integration_set_get_delete():
    await _require_live_redis()
    storage = PredictionRedisStorage()
    row_id = int(time.time() * 1000)
    key = f"prediction:{row_id}"
    payload = {"is_valid": True, "probability": 0.63}

    await storage.delete(row_id)
    await storage.set(row_id, payload)
    cached = await storage.get(row_id)

    async with get_redis_connection() as connection:
        ttl = await connection.ttl(key)

    assert cached == payload
    assert 0 < ttl <= 86400

    await storage.delete(row_id)
    assert await storage.get(row_id) is None


@pytest.mark.asyncio
async def test_moderation_storage_integration_pending_and_terminal_ttl():
    await _require_live_redis()
    storage = ModerationResultRedisStorage()
    row_id = int(time.time() * 1000)
    key = f"moderation_result:{row_id}"

    await storage.delete(row_id)
    pending_payload = {"task_id": row_id, "status": "pending", "is_violation": None, "probability": None}
    await storage.set(row_id, pending_payload)
    pending_cached = await storage.get(row_id)

    async with get_redis_connection() as connection:
        pending_ttl = await connection.ttl(key)

    terminal_payload = {"task_id": row_id, "status": "failed", "is_violation": None, "probability": None}
    await storage.set(row_id, terminal_payload)
    terminal_cached = await storage.get(row_id)

    async with get_redis_connection() as connection:
        terminal_ttl = await connection.ttl(key)

    assert pending_cached == pending_payload
    assert terminal_cached == terminal_payload
    assert 0 < pending_ttl <= 15
    assert 15 < terminal_ttl <= 86400

    await storage.delete(row_id)
    assert await storage.get(row_id) is None
