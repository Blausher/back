import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.clients.model import ModelNotLoadedError
from app.models.advertisement import Advertisement
from app.models.moderation_result import ModerationResult
from app.routers import predict as predict_router
from app.services import moderation

client = TestClient(app)

VALID_PAYLOAD = {
    "seller_id": 1,
    "is_verified_seller": False,
    "item_id": 42,
    "name": "Office chair",
    "description": "Comfortable chair with wheels",
    "category": 3,
    "images_qty": 0,
}


def test_predict_positive_valid(monkeypatch):
    '''
    положительный результат предсказания (валидное объявление)
    '''
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.87,
    )

    payload = {**VALID_PAYLOAD, "is_verified_seller": True, "images_qty": 0}

    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["is_valid"] is True
    assert body["probability"] == 0.87


def test_predict_negative_invalid(monkeypatch):
    '''
    отрицательный результат предсказания (невалидное объявление)
    '''
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.12,
    )

    payload = {**VALID_PAYLOAD, "is_verified_seller": False, "images_qty": 0}

    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["is_valid"] is False
    assert body["probability"] == 0.12


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
def test_predict_validation_error_on_invalid_values(patch, _label):
    '''
    валидация значений (тип, содержимое)
    '''
    payload = {**VALID_PAYLOAD, **patch}

    response = client.post("/predict", json=payload)

    assert response.status_code == 422


@pytest.mark.parametrize("missing_field", MISSING_REQUIRED_FIELDS)
def test_predict_validation_error_on_missing_field(missing_field):
    '''
    валидация обязательных аргументов
    '''
    payload = {**VALID_PAYLOAD}
    payload.pop(missing_field)

    response = client.post("/predict", json=payload)

    assert response.status_code == 422


def test_predict_business_logic_error(monkeypatch):
    """Проверяет ошибку бизнес-логики."""
    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        lambda _ad: 0.33,
    )

    def raise_error(_):
        raise moderation.BusinessLogicError("boom")

    monkeypatch.setattr(moderation, "predict_has_violations", raise_error)

    response = client.post("/predict", json=VALID_PAYLOAD)

    assert response.status_code == 500
    assert response.json()["detail"] == "Business logic prediction failed"


def test_predict_model_unavailable(monkeypatch):
    """Проверяет ответ при отсутствии модели."""
    def raise_not_loaded(_ad):
        raise ModelNotLoadedError("Model is not loaded")

    monkeypatch.setattr(
        predict_router.prediction.model_client,
        "predict_probability",
        raise_not_loaded,
    )

    response = client.post("/predict", json=VALID_PAYLOAD)

    assert response.status_code == 503
    assert response.json()["detail"] == "Model is not loaded"


def test_simple_predict_success(monkeypatch):
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

    response = client.get("/simple_predict", params={"item_id": 42})

    assert response.status_code == 200
    body = response.json()
    assert body["is_valid"] is True
    assert body["probability"] == 0.87


def test_simple_predict_not_found(monkeypatch):
    """Проверяет 404 при отсутствии объявления."""
    class DummyRepo:
        async def select_advert(self, _item_id):
            return None

    monkeypatch.setattr(predict_router, "advertisement_repo", DummyRepo())

    response = client.get("/simple_predict", params={"item_id": 404})

    assert response.status_code == 404
    assert response.json()["detail"] == "Advertisement not found"


def test_moderation_result_pending(monkeypatch):
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

    response = client.get("/moderation_result/123")

    assert response.status_code == 200
    assert response.json() == {
        "task_id": 123,
        "status": "pending",
        "is_violation": None,
        "probability": None,
    }


def test_moderation_result_completed(monkeypatch):
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

    response = client.get("/moderation_result/124")

    assert response.status_code == 200
    assert response.json() == {
        "task_id": 124,
        "status": "completed",
        "is_violation": True,
        "probability": 0.87,
    }


def test_moderation_result_not_found(monkeypatch):
    class DummyRepo:
        async def get_by_id(self, _task_id):
            return None

    monkeypatch.setattr(predict_router, "moderation_result_repo", DummyRepo())

    response = client.get("/moderation_result/999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Moderation task not found"
