import pytest

from app.models.async_predict import AsyncPredictRequest
from app.models.moderation_result import ModerationResult
from app.routers import predict as predict_router


@pytest.fixture(autouse=True)
def cache_storage_stub(monkeypatch):
    class DummyPredictionCache:
        async def get(self, _item_id):
            return None

        async def set(self, _item_id, _row):
            return None

    class DummyModerationCache:
        async def get(self, _task_id):
            return None

        async def set(self, _task_id, _row):
            return None

    monkeypatch.setattr(predict_router, "prediction_cache_storage", DummyPredictionCache())
    monkeypatch.setattr(predict_router, "moderation_result_cache_storage", DummyModerationCache())


@pytest.mark.asyncio
async def test_async_predict_creates_task_and_sends_kafka(monkeypatch):
    """Проверяет создание pending-задачи и отправку сообщения в Kafka."""
    created_tasks = []
    sent_messages = []

    class DummyAdsRepo:
        async def select_advert(self, item_id):
            return {
                "seller_id": 1,
                "is_verified_seller": True,
                "item_id": item_id,
                "name": "Desk",
                "description": "Wooden desk",
                "category": 3,
                "images_qty": 2,
            }

    class DummyModerationRepo:
        async def create_pending(self, item_id):
            created_tasks.append(item_id)
            return ModerationResult.model_validate(
                {
                    "id": 501,
                    "item_id": item_id,
                    "status": "pending",
                    "is_violation": None,
                    "probability": None,
                    "error_message": None,
                    "created_at": None,
                    "processed_at": None,
                }
            )

    class DummyKafkaClient:
        async def send_moderation_request(self, item_id):
            sent_messages.append(item_id)

    monkeypatch.setattr(predict_router, "advertisement_repo", DummyAdsRepo())
    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyModerationRepo())
    monkeypatch.setattr(predict_router, "kafka_client", DummyKafkaClient())

    response = await predict_router.async_predict(AsyncPredictRequest(item_id=42))

    assert response == {
        "task_id": 501,
        "status": "pending",
        "message": "Moderation request accepted",
    }
    assert created_tasks == [42]
    assert sent_messages == [42]


@pytest.mark.asyncio
async def test_moderation_result_returns_failed_status(monkeypatch):
    """Проверяет выдачу статуса failed из moderation_result."""
    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": 502,
                    "item_id": 42,
                    "status": "failed",
                    "is_violation": None,
                    "probability": None,
                    "error_message": "Advertisement not found",
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(502)

    assert response == {
        "task_id": 502,
        "status": "failed",
        "is_violation": None,
        "probability": None,
    }
