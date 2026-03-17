import logging

from app.clients.model import ModelClient
from app.errors import AdvertisementNotFoundError
from app.models.advertisement import Advertisement
from app.observability.metrics import PREDICTIONS_TOTAL
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.prediction_cache import PredictionRedisStorage
from app.services import moderation


logger = logging.getLogger(__name__)

# Singleton-клиент модели на сервисном уровне.
model_client = ModelClient()
advertisement_repo = AdvertisementRepository()
prediction_cache_storage = PredictionRedisStorage()


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
