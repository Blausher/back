import logging

from fastapi import APIRouter, HTTPException, Path

from app.clients.kafka import KafkaProducerClient
from app.clients.model import ModelInferenceError, ModelNotLoadedError
from app.errors import StorageUnavailableError
from app.models.advertisement import Advertisement
from app.models.async_predict import (
    AsyncPredictRequest,
    AsyncPredictResponse,
    ModerationResultResponse,
)
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.moderation_results import ModerationResultRepository
from app.services import moderation, prediction

router = APIRouter()
logger = logging.getLogger(__name__)
advertisement_repo = AdvertisementRepository()
moderation_result_repo = ModerationResultRepository()
kafka_client = KafkaProducerClient()


@router.post("/predict")
async def predict(advertisement: Advertisement) -> dict:
    """
    Возвращает валидность объявления и вероятность.
    """
    is_valid, probability = _predict(advertisement)

    logger.info(
        "Predict result seller_id=%s item_id=%s is_valid=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        is_valid,
        probability,
    )

    return {"is_valid": is_valid, "probability": probability}


@router.get("/simple_predict")
async def simple_predict(item_id: int) -> dict:
    """
    Возвращает валидность объявления по item_id.
    """
    try:
        advertisement = await advertisement_repo.select_advert(item_id)
    except StorageUnavailableError as exc:
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    if advertisement is None:
        raise HTTPException(status_code=404, detail="Advertisement not found")

    is_valid, probability = _predict(advertisement)

    logger.info(
        "Simple predict result seller_id=%s item_id=%s is_valid=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        is_valid,
        probability,
    )

    return {"is_valid": is_valid, "probability": probability}


@router.post("/async_predict", response_model=AsyncPredictResponse)
async def async_predict(payload: AsyncPredictRequest) -> dict:
    """
    Создает задачу на модерацию объявления по item_id и отправляет запрос в Kafka очередь.
    """
    try:
        advertisement = await advertisement_repo.select_advert(payload.item_id)
    except StorageUnavailableError as exc:
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    if advertisement is None:
        raise HTTPException(status_code=404, detail="Advertisement not found")

    try:
        moderation_result = await moderation_result_repo.create_pending(payload.item_id)
    except StorageUnavailableError as exc:
        logger.exception("Create moderation result failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc

    try:
        await kafka_client.send_moderation_request(payload.item_id)
    except Exception as exc:
        logger.exception("Kafka send failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc

    return {
        "task_id": moderation_result.id,
        "status": moderation_result.status,
        "message": "Moderation request accepted",
    }


@router.get("/moderation_result/{task_id}", response_model=ModerationResultResponse)
async def moderation_result(task_id: int = Path(ge=0)) -> dict:
    """
    Возвращает статус задачи модерации по task_id.
    """
    try:
        result = await moderation_result_repo.get_by_id(task_id)
    except StorageUnavailableError as exc:
        logger.exception("Get moderation result failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc

    if result is None:
        raise HTTPException(status_code=404, detail="Moderation task not found")

    return {
        "task_id": result.id,
        "status": result.status,
        "is_violation": result.is_violation,
        "probability": result.probability,
    }


def _predict(advertisement: Advertisement) -> tuple[bool, float]:
    try:
        probability = prediction.model_client.predict_probability(advertisement)
    except ModelNotLoadedError as exc:
        raise HTTPException(
            status_code=503,
            detail="Model is not loaded",
        ) from exc
    except ModelInferenceError as exc:
        raise HTTPException(
            status_code=500,
            detail="Model inference failed",
        ) from exc

    logger.info(
        "Predict request seller_id=%s item_id=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        probability,
    )

    try:
        is_valid = moderation.predict_has_violations(advertisement)
    except moderation.BusinessLogicError as exc:
        raise HTTPException(
            status_code=500,
            detail="Business logic prediction failed",
        ) from exc

    return is_valid, probability
