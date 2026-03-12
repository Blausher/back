import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.clients.model import ModelNotLoadedError
from app.models.account import Account
from app.models.advertisement import Advertisement
from app.models.moderation_result import ModerationResult
from app.routers import predict as predict_router
from app.services import moderation

VALID_PAYLOAD = {
    "seller_id": 1,
    "is_verified_seller": False,
    "item_id": 42,
    "name": "Office chair",
    "description": "Comfortable chair with wheels",
    "category": 3,
    "images_qty": 0,
}

AUTHENTICATED_ACCOUNT = Account(
    id=1,
    login="tester",
    password="hashed-password",
    is_blocked=False,
)

pytestmark = pytest.mark.asyncio


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
async def test_predict_positive_valid(monkeypatch):
    '''
    положительный результат предсказания (валидное объявление)
    '''
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.87,
    )

    payload = {**VALID_PAYLOAD, "is_verified_seller": True, "images_qty": 0}

    response = await predict_router.predict(
        Advertisement.model_validate(payload),
        AUTHENTICATED_ACCOUNT,
    )

    assert response["is_valid"] is True
    assert response["probability"] == 0.87


async def test_predict_negative_invalid(monkeypatch):
    '''
    отрицательный результат предсказания (невалидное объявление)
    '''
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.12,
    )

    payload = {**VALID_PAYLOAD, "is_verified_seller": False, "images_qty": 0}

    response = await predict_router.predict(
        Advertisement.model_validate(payload),
        AUTHENTICATED_ACCOUNT,
    )

    assert response["is_valid"] is False
    assert response["probability"] == 0.12


INVALID_PAYLOADS = [
    ({"seller_id": "abc"}, "seller_id"),
    ({"is_verified_seller": {"yes": True}}, "is_verified_seller"),
    ({"item_id": []}, "item_id"),
    ({"name": 123}, "name"),
    ({"description": 123}, "description"),
    ({"category": "x"}, "category"),
    ({"images_qty": []}, "images_qty"),
]

MISSING_REQUIRED_FIELDS = [
    "seller_id",
    "is_verified_seller",
    "item_id",
    "name",
    "description",
    "category",
    "images_qty",
]


@pytest.mark.parametrize("patch, _label", INVALID_PAYLOADS)
async def test_predict_validation_error_on_invalid_values(patch, _label):
    '''
    валидация значений (тип, содержимое)
    '''
    payload = {**VALID_PAYLOAD, **patch}

    with pytest.raises(ValidationError):
        Advertisement.model_validate(payload)


@pytest.mark.parametrize("missing_field", MISSING_REQUIRED_FIELDS)
async def test_predict_validation_error_on_missing_field(missing_field):
    '''
    валидация обязательных аргументов
    '''
    payload = {**VALID_PAYLOAD}
    payload.pop(missing_field)

    with pytest.raises(ValidationError):
        Advertisement.model_validate(payload)


async def test_predict_business_logic_error(monkeypatch):
    """Проверяет ошибку бизнес-логики."""
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.33,
    )

    def raise_error(_):
        raise moderation.BusinessLogicError("boom")

    monkeypatch.setattr(moderation, "predict_has_violations", raise_error)

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.predict(
            Advertisement.model_validate(VALID_PAYLOAD),
            AUTHENTICATED_ACCOUNT,
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Business logic prediction failed"


async def test_predict_model_unavailable(monkeypatch):
    """Проверяет ответ при отсутствии модели."""
    def raise_not_loaded(_ad):
        raise ModelNotLoadedError("Model is not loaded")

    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        raise_not_loaded,
    )

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.predict(
            Advertisement.model_validate(VALID_PAYLOAD),
            AUTHENTICATED_ACCOUNT,
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Model is not loaded"


async def test_simple_predict_success(monkeypatch):
    """Проверяет успешный simple_predict."""
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.87,
    )
    monkeypatch.setattr(moderation, "predict_has_violations", lambda _: True)

    class DummyRepo:
        async def select_advert(self, _item_id):
            return Advertisement.model_validate(VALID_PAYLOAD)

    monkeypatch.setattr(predict_router, "advertisement_repo", DummyRepo())

    response = await predict_router.simple_predict(42, AUTHENTICATED_ACCOUNT)

    assert response["is_valid"] is True
    assert response["probability"] == 0.87


async def test_simple_predict_not_found(monkeypatch):
    """Проверяет 404 при отсутствии объявления."""
    class DummyRepo:
        async def select_advert(self, _item_id):
            return None

    monkeypatch.setattr(predict_router, "advertisement_repo", DummyRepo())

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.simple_predict(404, AUTHENTICATED_ACCOUNT)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Advertisement not found"


async def test_simple_predict_returns_from_cache_without_db_and_model(monkeypatch):
    class DummyCache:
        async def get(self, _item_id):
            return {"is_valid": True, "probability": 0.99}

        async def set(self, _item_id, _row):
            raise AssertionError("set should not be called on cache hit")

    class DummyRepo:
        async def select_advert(self, _item_id):
            raise AssertionError("DB should not be called on cache hit")

    def fail_model(_ad):
        raise AssertionError("Model should not be called")

    monkeypatch.setattr(predict_router, "prediction_cache_storage", DummyCache())
    monkeypatch.setattr(predict_router, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        fail_model,
    )

    response = await predict_router.simple_predict(42, AUTHENTICATED_ACCOUNT)

    assert response == {"is_valid": True, "probability": 0.99}


async def test_simple_predict_cache_miss_saves_result(monkeypatch):
    cache_set_calls = []

    class DummyCache:
        async def get(self, _item_id):
            return None

        async def set(self, item_id, row):
            cache_set_calls.append((item_id, row))

    class DummyRepo:
        async def select_advert(self, _item_id):
            return Advertisement.model_validate(VALID_PAYLOAD)

    monkeypatch.setattr(predict_router, "prediction_cache_storage", DummyCache())
    monkeypatch.setattr(predict_router, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.77,
    )
    monkeypatch.setattr(moderation, "predict_has_violations", lambda _ad: False)

    response = await predict_router.simple_predict(42, AUTHENTICATED_ACCOUNT)

    assert response == {"is_valid": False, "probability": 0.77}
    assert cache_set_calls == [(42, {"is_valid": False, "probability": 0.77})]


async def test_moderation_result_pending(monkeypatch):
    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": 123,
                    "item_id": 42,
                    "status": "pending",
                    "is_violation": None,
                    "probability": None,
                    "error_message": None,
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(123, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": 123,
        "status": "pending",
        "is_violation": None,
        "probability": None,
    }


async def test_moderation_result_completed(monkeypatch):
    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": 124,
                    "item_id": 42,
                    "status": "completed",
                    "is_violation": True,
                    "probability": 0.87,
                    "error_message": None,
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(124, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": 124,
        "status": "completed",
        "is_violation": True,
        "probability": 0.87,
    }


async def test_moderation_result_not_found(monkeypatch):
    class DummyRepo:
        async def get_by_id(self, _task_id):
            return None

    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.moderation_result(999, AUTHENTICATED_ACCOUNT)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Moderation task not found"


async def test_moderation_result_returns_from_cache_without_db(monkeypatch):
    class DummyCache:
        async def get(self, _task_id):
            return {
                "task_id": 777,
                "status": "completed",
                "is_violation": True,
                "probability": 0.88,
            }

        async def set(self, _task_id, _row):
            raise AssertionError("set should not be called on cache hit")

    class DummyRepo:
        async def get_by_id(self, _task_id):
            raise AssertionError("DB should not be called on cache hit")

    monkeypatch.setattr(predict_router, "moderation_result_cache_storage", DummyCache())
    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(777, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": 777,
        "status": "completed",
        "is_violation": True,
        "probability": 0.88,
    }


async def test_moderation_result_cache_miss_saves_result(monkeypatch):
    cache_set_calls = []

    class DummyCache:
        async def get(self, _task_id):
            return None

        async def set(self, task_id, row):
            cache_set_calls.append((task_id, row))

    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": 778,
                    "item_id": 42,
                    "status": "failed",
                    "is_violation": None,
                    "probability": None,
                    "error_message": "Advertisement not found",
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(predict_router, "moderation_result_cache_storage", DummyCache())
    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(778, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": 778,
        "status": "failed",
        "is_violation": None,
        "probability": None,
    }
    assert cache_set_calls == [
        (
            778,
            {
                "task_id": 778,
                "status": "failed",
                "is_violation": None,
                "probability": None,
            },
        )
    ]
