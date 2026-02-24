import pytest
from fastapi import HTTPException

from app.errors import StorageUnavailableError
from app.models.close_advertisement import CloseAdvertisementRequest
from app.repositories.advertisements import AdvertisementCloseResult
from app.routers import entities as entities_router


@pytest.mark.asyncio
async def test_close_advertisement_success(monkeypatch):
    prediction_deleted = []
    moderation_deleted = []

    class DummyRepo:
        async def close(self, _item_id):
            return AdvertisementCloseResult(item_id=42, moderation_result_ids=[501, 502])

    class DummyPredictionCache:
        async def delete(self, row_id):
            prediction_deleted.append(row_id)

    class DummyModerationCache:
        async def delete(self, row_id):
            moderation_deleted.append(row_id)

    monkeypatch.setattr(entities_router, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(entities_router, "prediction_cache_storage", DummyPredictionCache())
    monkeypatch.setattr(entities_router, "moderation_result_cache_storage", DummyModerationCache())

    response = await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=42))

    assert response == {
        "item_id": 42,
        "status": "closed",
        "message": "Advertisement closed",
    }
    assert prediction_deleted == [42]
    assert moderation_deleted == [501, 502]


@pytest.mark.asyncio
async def test_close_advertisement_not_found(monkeypatch):
    class DummyRepo:
        async def close(self, _item_id):
            return None

    class DummyPredictionCache:
        async def delete(self, _row_id):
            raise AssertionError("Prediction cache delete should not be called")

    class DummyModerationCache:
        async def delete(self, _row_id):
            raise AssertionError("Moderation cache delete should not be called")

    monkeypatch.setattr(entities_router, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(entities_router, "prediction_cache_storage", DummyPredictionCache())
    monkeypatch.setattr(entities_router, "moderation_result_cache_storage", DummyModerationCache())

    with pytest.raises(HTTPException) as exc:
        await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=404))

    assert exc.value.status_code == 404
    assert exc.value.detail == "Advertisement not found"


@pytest.mark.asyncio
async def test_close_advertisement_storage_unavailable(monkeypatch):
    class DummyRepo:
        async def close(self, _item_id):
            raise StorageUnavailableError("Storage operation failed")

    monkeypatch.setattr(entities_router, "advertisement_repo", DummyRepo())

    with pytest.raises(HTTPException) as exc:
        await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=42))

    assert exc.value.status_code == 500
    assert exc.value.detail == "Internal server error"


@pytest.mark.asyncio
async def test_close_advertisement_redis_failures_are_best_effort(monkeypatch):
    prediction_attempts = []
    moderation_attempts = []

    class DummyRepo:
        async def close(self, _item_id):
            return AdvertisementCloseResult(item_id=42, moderation_result_ids=[601, 602])

    class FailingPredictionCache:
        async def delete(self, row_id):
            prediction_attempts.append(row_id)
            raise RuntimeError("redis down")

    class FailingModerationCache:
        async def delete(self, row_id):
            moderation_attempts.append(row_id)
            raise RuntimeError("redis down")

    monkeypatch.setattr(entities_router, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(entities_router, "prediction_cache_storage", FailingPredictionCache())
    monkeypatch.setattr(entities_router, "moderation_result_cache_storage", FailingModerationCache())

    response = await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=42))

    assert response == {
        "item_id": 42,
        "status": "closed",
        "message": "Advertisement closed",
    }
    assert prediction_attempts == [42]
    assert moderation_attempts == [601, 602]
