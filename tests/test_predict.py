from fastapi.testclient import TestClient

from main import app
from services import moderation

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


def test_predict_positive_violation():
    '''
    положительный результат предсказания
    '''
    payload = {**VALID_PAYLOAD, "is_verified_seller": False, "images_qty": 0}

    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    assert response.json() is True


def test_predict_negative_no_violation():
    '''
    отрицательный результат предсказания
    '''
    payload = {**VALID_PAYLOAD, "is_verified_seller": True, "images_qty": 0}

    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    assert response.json() is False


def test_predict_validation_error_on_type():
    '''
    валидация значений
    '''
    payload = {**VALID_PAYLOAD, "seller_id": "abc"}

    response = client.post("/predict", json=payload)

    assert response.status_code == 422


def test_predict_business_logic_error(monkeypatch):
    def raise_error(_):
        raise moderation.BusinessLogicError("boom")

    monkeypatch.setattr(moderation, "predict_has_violations", raise_error)

    response = client.post("/predict", json=VALID_PAYLOAD)

    assert response.status_code == 500
    assert response.json()["detail"] == "Business logic error"
