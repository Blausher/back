import pytest
from fastapi import HTTPException

from app.errors import StorageUnavailableError
from app.models.close_advertisement import CloseAdvertisementRequest
from app.repositories.advertisements import AdvertisementCloseResult
from app.routers import entities as entities_router
from tests.id_factory import new_id


@pytest.mark.asyncio
async def test_close_advertisement_success(monkeypatch):
    """Успешно закрывает объявление и очищает связанные ключи в Redis."""
    prediction_deleted = []
    moderation_deleted = []
    item_id = new_id()
    moderation_result_ids = [new_id(), new_id()]

    class DummyRepo:
        async def close(self, _item_id):
            return AdvertisementCloseResult(item_id=item_id, moderation_result_ids=moderation_result_ids)

    class DummyPredictionCache:
        async def delete(self, row_id):
            prediction_deleted.append(row_id)

    class DummyModerationCache:
        async def delete(self, row_id):
            moderation_deleted.append(row_id)

    monkeypatch.setattr(entities_router, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(entities_router, "prediction_cache_storage", DummyPredictionCache())
    monkeypatch.setattr(entities_router, "moderation_result_cache_storage", DummyModerationCache())

    response = await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=item_id))

    assert response == {
        "item_id": item_id,
        "status": "closed",
        "message": "Advertisement closed",
    }
    assert prediction_deleted == [item_id]
    assert moderation_deleted == moderation_result_ids


@pytest.mark.asyncio
async def test_close_advertisement_not_found(monkeypatch):
    """Возвращает 404, если объявление для закрытия не найдено."""
    missing_item_id = new_id()

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
        await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=missing_item_id))

    assert exc.value.status_code == 404
    assert exc.value.detail == "Advertisement not found"


@pytest.mark.asyncio
async def test_close_advertisement_storage_unavailable(monkeypatch):
    """Возвращает 500 при ошибке PostgreSQL в репозитории."""
    item_id = new_id()

    class DummyRepo:
        async def close(self, _item_id):
            raise StorageUnavailableError("Storage operation failed")

    monkeypatch.setattr(entities_router, "advertisement_repo", DummyRepo())

    with pytest.raises(HTTPException) as exc:
        await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=item_id))

    assert exc.value.status_code == 500
    assert exc.value.detail == "Internal server error"


@pytest.mark.asyncio
async def test_close_advertisement_redis_failures_are_best_effort(monkeypatch):
    """Не падает, если Redis недоступен после успешного удаления в БД."""
    prediction_attempts = []
    moderation_attempts = []
    item_id = new_id()
    moderation_result_ids = [new_id(), new_id()]

    class DummyRepo:
        async def close(self, _item_id):
            return AdvertisementCloseResult(item_id=item_id, moderation_result_ids=moderation_result_ids)

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

    response = await entities_router.close_advertisement(CloseAdvertisementRequest(item_id=item_id))

    assert response == {
        "item_id": item_id,
        "status": "closed",
        "message": "Advertisement closed",
    }
    assert prediction_attempts == [item_id]
    assert moderation_attempts == moderation_result_ids
