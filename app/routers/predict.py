import logging

from fastapi import APIRouter, Depends, HTTPException, Path

from app.clients.model import ModelInferenceError, ModelNotLoadedError
from app.dependencies import require_account
from app.errors import (
    AdvertisementNotFoundError,
    ModerationTaskNotFoundError,
    StorageUnavailableError,
)
from app.models.account import Account
from app.models.advertisement import Advertisement
from app.models.async_predict import (
    AsyncPredictRequest,
    AsyncPredictResponse,
    ModerationResultResponse,
)
from app.services import moderation, prediction

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/predict")
async def predict(
    advertisement: Advertisement,
    _account: Account = Depends(require_account),
) -> dict:
    """
    Возвращает валидность объявления и вероятность.
    """
    try:
        return prediction.predict_advertisement(advertisement)
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
    except moderation.BusinessLogicError as exc:
        raise HTTPException(
            status_code=500,
            detail="Business logic prediction failed",
        ) from exc


@router.get("/simple_predict")
async def simple_predict(
    item_id: int,
    _account: Account = Depends(require_account),
) -> dict:
    """
    Возвращает валидность объявления по item_id.
    """
    try:
        return await prediction.simple_predict_by_item_id(item_id)
    except AdvertisementNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Advertisement not found") from exc
    except StorageUnavailableError as exc:
        raise HTTPException(status_code=500, detail="Internal server error") from exc
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
    except moderation.BusinessLogicError as exc:
        raise HTTPException(
            status_code=500,
            detail="Business logic prediction failed",
        ) from exc


@router.post("/async_predict", response_model=AsyncPredictResponse)
async def async_predict(
    payload: AsyncPredictRequest,
    _account: Account = Depends(require_account),
) -> dict:
    """
    Создает задачу на модерацию объявления по item_id и отправляет запрос в Kafka очередь.
    """
    try:
        return await moderation.request_moderation(payload.item_id)
    except AdvertisementNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Advertisement not found") from exc
    except StorageUnavailableError as exc:
        raise HTTPException(status_code=500, detail="Internal server error") from exc
    except moderation.ModerationRequestError as exc:
        raise HTTPException(status_code=500, detail="Internal server error") from exc


@router.get("/moderation_result/{task_id}", response_model=ModerationResultResponse)
async def moderation_result(
    task_id: int = Path(ge=0),
    _account: Account = Depends(require_account),
) -> dict:
    """
    Возвращает статус задачи модерации по task_id.
    """
    try:
        return await moderation.get_moderation_result(task_id)
    except ModerationTaskNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Moderation task not found") from exc
    except StorageUnavailableError as exc:
        logger.exception("Get moderation result failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc
