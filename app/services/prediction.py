import logging

from app.clients.model import ModelClient
from app.errors import AdvertisementNotFoundError, ModerationTaskNotFoundError
from app.models.advertisement import Advertisement
from app.observability.metrics import PREDICTIONS_TOTAL
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.moderation_results import ModerationResultRepository
from app.repositories.prediction_cache import (
    ModerationResultRedisStorage,
    PredictionRedisStorage,
)
from app.services import moderation


logger = logging.getLogger(__name__)

# Singleton-клиент модели на сервисном уровне.
model_client = ModelClient()
advertisement_repo = AdvertisementRepository()
moderation_result_repo = ModerationResultRepository()
prediction_cache_storage = PredictionRedisStorage()
moderation_result_cache_storage = ModerationResultRedisStorage()


def predict_advertisement(advertisement: Advertisement) -> dict:
    probability = model_client.predict_probability(advertisement)

    PREDICTIONS_TOTAL.labels(
        result="violation" if probability >= 0.5 else "no_violation",
    ).inc()

    logger.info(
        "Predict request seller_id=%s item_id=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        probability,
    )

    is_valid = moderation.predict_has_violations(advertisement)
    response = {"is_valid": is_valid, "probability": probability}

    logger.info(
        "Predict result seller_id=%s item_id=%s is_valid=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        is_valid,
        probability,
    )

    return response


async def simple_predict_by_item_id(item_id: int) -> dict:
    cached_result = await _get_cached_prediction(item_id)
    if cached_result is not None:
        logger.info("Simple predict cache hit item_id=%s", item_id)
        return cached_result

    advertisement = await advertisement_repo.select_advert(item_id)
    if advertisement is None:
        raise AdvertisementNotFoundError("Advertisement not found")

    response = predict_advertisement(advertisement)
    await _set_cached_prediction(item_id, response)
    return response


async def get_moderation_result(task_id: int) -> dict:
    cached_result = await _get_cached_moderation_result(task_id)
    if cached_result is not None:
        logger.info("Moderation result cache hit task_id=%s", task_id)
        return cached_result

    result = await moderation_result_repo.get_by_id(task_id)
    if result is None:
        raise ModerationTaskNotFoundError("Moderation task not found")

    response = {
        "task_id": result.id,
        "status": result.status,
        "is_violation": result.is_violation,
        "probability": result.probability,
    }
    await _set_cached_moderation_result(task_id, response)
    return response


async def _get_cached_prediction(item_id: int) -> dict | None:
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
    try:
        await prediction_cache_storage.set(item_id, row)
    except Exception:
        logger.exception("Prediction cache set failed item_id=%s", item_id)


async def _get_cached_moderation_result(task_id: int) -> dict | None:
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
    try:
        await moderation_result_cache_storage.set(task_id, row)
    except Exception:
        logger.exception("Moderation result cache set failed task_id=%s", task_id)
