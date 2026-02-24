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
from app.repositories.prediction_cache import (
    ModerationResultRedisStorage,
    PredictionRedisStorage,
)
from app.services import moderation, prediction

router = APIRouter()
logger = logging.getLogger(__name__)
advertisement_repo = AdvertisementRepository()
moderation_result_repo = ModerationResultRepository()
prediction_cache_storage = PredictionRedisStorage()
moderation_result_cache_storage = ModerationResultRedisStorage()
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
    cached_result = await _get_cached_prediction(item_id)
    if cached_result is not None:
        logger.info("Simple predict cache hit item_id=%s", item_id)
        return cached_result

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

    response = {"is_valid": is_valid, "probability": probability}
    await _set_cached_prediction(item_id, response)
    return response


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
    cached_result = await _get_cached_moderation_result(task_id)
    if cached_result is not None:
        logger.info("Moderation result cache hit task_id=%s", task_id)
        return cached_result

    try:
        result = await moderation_result_repo.get_by_id(task_id)
    except StorageUnavailableError as exc:
        logger.exception("Get moderation result failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc

    if result is None:
        raise HTTPException(status_code=404, detail="Moderation task not found")

    response = {
        "task_id": result.id,
        "status": result.status,
        "is_violation": result.is_violation,
        "probability": result.probability,
    }
    await _set_cached_moderation_result(task_id, response)
    return response


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


async def _get_cached_prediction(item_id: int) -> dict | None:
    """Читает результат simple_predict из Redis по item_id."""
    try:
        cached_row = await prediction_cache_storage.get(item_id)
    except Exception:
        logger.exception("Prediction cache get failed item_id=%s", item_id)
        return None

    if cached_row is None:
        return None

    if "is_valid" not in cached_row or "probability" not in cached_row:
        logger.warning("Prediction cache payload is invalid item_id=%s", item_id)
        return None

    return {
        "is_valid": cached_row["is_valid"],
        "probability": cached_row["probability"],
    }


async def _set_cached_prediction(item_id: int, row: dict) -> None:
    """Сохраняет результат simple_predict в Redis по item_id."""
    try:
        await prediction_cache_storage.set(item_id, row)
    except Exception:
        logger.exception("Prediction cache set failed item_id=%s", item_id)


async def _get_cached_moderation_result(task_id: int) -> dict | None:
    """Читает статус moderation_result из Redis по task_id."""
    try:
        cached_row = await moderation_result_cache_storage.get(task_id)
    except Exception:
        logger.exception("Moderation result cache get failed task_id=%s", task_id)
        return None

    if cached_row is None:
        return None

    if "task_id" not in cached_row or "status" not in cached_row:
        logger.warning("Moderation result cache payload is invalid task_id=%s", task_id)
        return None

    return {
        "task_id": cached_row["task_id"],
        "status": cached_row["status"],
        "is_violation": cached_row.get("is_violation"),
        "probability": cached_row.get("probability"),
    }


async def _set_cached_moderation_result(task_id: int, row: dict) -> None:
    """Сохраняет статус moderation_result в Redis по task_id."""
    try:
        await moderation_result_cache_storage.set(task_id, row)
    except Exception:
        logger.exception("Moderation result cache set failed task_id=%s", task_id)
