import logging

from app.clients.kafka import kafka_client
from app.errors import AdvertisementNotFoundError, ModerationTaskNotFoundError
from app.models.advertisement import Advertisement
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.moderation_results import ModerationResultRepository
from app.repositories.prediction_cache import ModerationResultRedisStorage


logger = logging.getLogger(__name__)

advertisement_repo = AdvertisementRepository()
moderation_result_repo = ModerationResultRepository()
moderation_result_cache_storage = ModerationResultRedisStorage()


class BusinessLogicError(RuntimeError):
    """Ошибка бизнес-правил модерации."""

    pass


class ModerationRequestError(RuntimeError):
    """Ошибка отправки задачи модерации во внешнюю очередь."""

    pass


def predict_has_violations(ad: Advertisement) -> bool:
    """
    Возвращает итоговую валидность объявления по бизнес-правилам.

    Подтвержденные продавцы считаются публикующими валидные объявления,
    неподтвержденные — невалидные.
    """
    return ad.is_verified_seller


async def request_moderation(item_id: int) -> dict:
    """Создает pending-задачу модерации и отправляет событие в Kafka."""

    advertisement = await advertisement_repo.select_advert(item_id)
    if advertisement is None:
        raise AdvertisementNotFoundError("Advertisement not found")

    moderation_result = await moderation_result_repo.create_pending(item_id)

    try:
        await kafka_client.send_moderation_request(item_id)
    except Exception as exc:
        logger.exception("Kafka send failed item_id=%s", item_id)
        raise ModerationRequestError("Failed to send moderation request") from exc

    return {
        "task_id": moderation_result.id,
        "status": moderation_result.status,
        "message": "Moderation request accepted",
    }


async def get_moderation_result(task_id: int) -> dict:
    """Возвращает результат модерации, сначала пытаясь прочитать его из кэша."""

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


async def _get_cached_moderation_result(task_id: int) -> dict | None:
    """Читает результат модерации из кэша и валидирует минимальный набор полей."""

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
    """Сохраняет результат модерации в кэш, не пробрасывая сбои кэша наружу."""

    try:
        await moderation_result_cache_storage.set(task_id, row)
    except Exception:
        logger.exception("Moderation result cache set failed task_id=%s", task_id)
