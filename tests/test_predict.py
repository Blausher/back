import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.clients.model import ModelNotLoadedError
from app.models.account import Account
from app.models.advertisement import Advertisement
from app.models.moderation_result import ModerationResult
from app.routers import predict as predict_router
from app.services import moderation
from tests.id_factory import new_id


def make_valid_payload(**overrides):
    payload = {
        "seller_id": new_id(),
        "is_verified_seller": False,
        "item_id": new_id(),
        "name": "Office chair",
        "description": "Comfortable chair with wheels",
        "category": 3,
        "images_qty": 0,
    }
    payload.update(overrides)
    return payload


AUTHENTICATED_ACCOUNT = Account(
    id=new_id(),
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

    monkeypatch.setattr(predict_router.prediction, "prediction_cache_storage", DummyPredictionCache())
    monkeypatch.setattr(
        predict_router.moderation,
        "moderation_result_cache_storage",
        DummyModerationCache(),
    )


async def test_predict_positive_valid(monkeypatch):
    '''
    положительный результат предсказания (валидное объявление)
    '''
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.87,
    )

    payload = make_valid_payload(is_verified_seller=True, images_qty=0)

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

    payload = make_valid_payload(is_verified_seller=False, images_qty=0)

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
    payload = make_valid_payload(**patch)

    with pytest.raises(ValidationError):
        Advertisement.model_validate(payload)


@pytest.mark.parametrize("missing_field", MISSING_REQUIRED_FIELDS)
async def test_predict_validation_error_on_missing_field(missing_field):
    '''
    валидация обязательных аргументов
    '''
    payload = make_valid_payload()
    payload.pop(missing_field)

    with pytest.raises(ValidationError):
        Advertisement.model_validate(payload)


async def test_predict_business_logic_error(monkeypatch):
    """Проверяет ошибку бизнес-логики."""
    captured = []

    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.33,
    )
    monkeypatch.setattr(
        predict_router.sentry_observability,
        "capture_exception",
        lambda exc, **kwargs: captured.append((exc, kwargs)),
    )

    def raise_error(_):
        raise moderation.BusinessLogicError("boom")

    monkeypatch.setattr(moderation, "predict_has_violations", raise_error)

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.predict(
            Advertisement.model_validate(make_valid_payload()),
            AUTHENTICATED_ACCOUNT,
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Business logic prediction failed"
    assert len(captured) == 1
    assert captured[0][1]["tags"]["endpoint"] == "predict"


async def test_predict_model_unavailable(monkeypatch):
    """Проверяет ответ при отсутствии модели."""
    captured = []

    def raise_not_loaded(_ad):
        raise ModelNotLoadedError("Model is not loaded")

    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        raise_not_loaded,
    )
    monkeypatch.setattr(
        predict_router.sentry_observability,
        "capture_exception",
        lambda exc, **kwargs: captured.append((exc, kwargs)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.predict(
            Advertisement.model_validate(make_valid_payload()),
            AUTHENTICATED_ACCOUNT,
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Model is not loaded"
    assert len(captured) == 1
    assert captured[0][1]["tags"]["endpoint"] == "predict"


async def test_simple_predict_success(monkeypatch):
    """Проверяет успешный simple_predict."""
    advertisement = Advertisement.model_validate(make_valid_payload())

    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.87,
    )
    monkeypatch.setattr(moderation, "predict_has_violations", lambda _: True)

    class DummyRepo:
        async def select_advert(self, _item_id):
            return advertisement

    monkeypatch.setattr(predict_router.prediction, "advertisement_repo", DummyRepo())

    response = await predict_router.simple_predict(advertisement.item_id, AUTHENTICATED_ACCOUNT)

    assert response["is_valid"] is True
    assert response["probability"] == 0.87


async def test_simple_predict_not_found(monkeypatch):
    """Проверяет 404 при отсутствии объявления."""
    missing_item_id = new_id()
    captured = []

    class DummyRepo:
        async def select_advert(self, _item_id):
            return None

    monkeypatch.setattr(predict_router.prediction, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(
        predict_router.sentry_observability,
        "capture_exception",
        lambda exc, **kwargs: captured.append((exc, kwargs)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.simple_predict(missing_item_id, AUTHENTICATED_ACCOUNT)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Advertisement not found"
    assert len(captured) == 1
    assert captured[0][1]["extras"] == {"item_id": missing_item_id}


async def test_simple_predict_returns_from_cache_without_db_and_model(monkeypatch):
    item_id = new_id()

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

    monkeypatch.setattr(predict_router.prediction, "prediction_cache_storage", DummyCache())
    monkeypatch.setattr(predict_router.prediction, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        fail_model,
    )

    response = await predict_router.simple_predict(item_id, AUTHENTICATED_ACCOUNT)

    assert response == {"is_valid": True, "probability": 0.99}


async def test_simple_predict_cache_miss_saves_result(monkeypatch):
    cache_set_calls = []
    advertisement = Advertisement.model_validate(make_valid_payload())

    class DummyCache:
        async def get(self, _item_id):
            return None

        async def set(self, item_id, row):
            cache_set_calls.append((item_id, row))

    class DummyRepo:
        async def select_advert(self, _item_id):
            return advertisement

    monkeypatch.setattr(predict_router.prediction, "prediction_cache_storage", DummyCache())
    monkeypatch.setattr(predict_router.prediction, "advertisement_repo", DummyRepo())
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.77,
    )
    monkeypatch.setattr(moderation, "predict_has_violations", lambda _ad: False)

    response = await predict_router.simple_predict(advertisement.item_id, AUTHENTICATED_ACCOUNT)

    assert response == {"is_valid": False, "probability": 0.77}
    assert cache_set_calls == [(advertisement.item_id, {"is_valid": False, "probability": 0.77})]


async def test_moderation_result_pending(monkeypatch):
    item_id = new_id()
    task_id = new_id()

    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": task_id,
                    "item_id": item_id,
                    "status": "pending",
                    "is_violation": None,
                    "probability": None,
                    "error_message": None,
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(predict_router.moderation, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(task_id, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": task_id,
        "status": "pending",
        "is_violation": None,
        "probability": None,
    }


async def test_moderation_result_completed(monkeypatch):
    item_id = new_id()
    task_id = new_id()

    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": task_id,
                    "item_id": item_id,
                    "status": "completed",
                    "is_violation": True,
                    "probability": 0.87,
                    "error_message": None,
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(predict_router.moderation, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(task_id, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": task_id,
        "status": "completed",
        "is_violation": True,
        "probability": 0.87,
    }


async def test_moderation_result_not_found(monkeypatch):
    missing_task_id = new_id()
    captured = []

    class DummyRepo:
        async def get_by_id(self, _task_id):
            return None

    monkeypatch.setattr(predict_router.moderation, "moderation_result_repo", DummyRepo())
    monkeypatch.setattr(
        predict_router.sentry_observability,
        "capture_exception",
        lambda exc, **kwargs: captured.append((exc, kwargs)),
    )

    with pytest.raises(HTTPException) as exc_info:
        await predict_router.moderation_result(missing_task_id, AUTHENTICATED_ACCOUNT)

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Moderation task not found"
    assert len(captured) == 1
    assert captured[0][1]["extras"] == {"task_id": missing_task_id}


async def test_moderation_result_returns_from_cache_without_db(monkeypatch):
    task_id = new_id()

    class DummyCache:
        async def get(self, _task_id):
            return {
                "task_id": task_id,
                "status": "completed",
                "is_violation": True,
                "probability": 0.88,
            }

        async def set(self, _task_id, _row):
            raise AssertionError("set should not be called on cache hit")

    class DummyRepo:
        async def get_by_id(self, _task_id):
            raise AssertionError("DB should not be called on cache hit")

    monkeypatch.setattr(
        predict_router.moderation,
        "moderation_result_cache_storage",
        DummyCache(),
    )
    monkeypatch.setattr(predict_router.moderation, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(task_id, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": task_id,
        "status": "completed",
        "is_violation": True,
        "probability": 0.88,
    }


async def test_moderation_result_cache_miss_saves_result(monkeypatch):
    cache_set_calls = []
    item_id = new_id()
    task_id = new_id()

    class DummyCache:
        async def get(self, _task_id):
            return None

        async def set(self, task_id, row):
            cache_set_calls.append((task_id, row))

    class DummyRepo:
        async def get_by_id(self, _task_id):
            return ModerationResult.model_validate(
                {
                    "id": task_id,
                    "item_id": item_id,
                    "status": "failed",
                    "is_violation": None,
                    "probability": None,
                    "error_message": "Advertisement not found",
                    "created_at": None,
                    "processed_at": None,
                }
            )

    monkeypatch.setattr(
        predict_router.moderation,
        "moderation_result_cache_storage",
        DummyCache(),
    )
    monkeypatch.setattr(predict_router.moderation, "moderation_result_repo", DummyRepo())

    response = await predict_router.moderation_result(task_id, AUTHENTICATED_ACCOUNT)

    assert response == {
        "task_id": task_id,
        "status": "failed",
        "is_violation": None,
        "probability": None,
    }
    assert cache_set_calls == [
        (
            task_id,
            {
                "task_id": task_id,
                "status": "failed",
                "is_violation": None,
                "probability": None,
            },
        )
    ]
