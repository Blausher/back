import logging

from fastapi import APIRouter, HTTPException

from app.errors import (
    AdvertisementAlreadyExistsError,
    SellerNotFoundError,
    StorageUnavailableError,
    UserAlreadyExistsError,
)
from app.models.advertisement import Advertisement
from app.models.advertisement_create import AdvertisementCreate
from app.models.close_advertisement import CloseAdvertisementRequest
from app.models.user import User
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.prediction_cache import (
    ModerationResultRedisStorage,
    PredictionRedisStorage,
)
from app.repositories.users import UserRepository

router = APIRouter()
logger = logging.getLogger(__name__)
advertisement_repo = AdvertisementRepository()
user_repo = UserRepository()
prediction_cache_storage = PredictionRedisStorage()
moderation_result_cache_storage = ModerationResultRedisStorage()


@router.post("/users", response_model=User)
async def create_user(user: User) -> User:
    '''
    Ручка для создания юзера
    '''
    try:
        return await user_repo.create(
            user_id=user.id,
            is_verified_seller=user.is_verified_seller,
        )
    except UserAlreadyExistsError as exc:
        raise HTTPException(status_code=409, detail="User already exists") from exc
    except StorageUnavailableError as exc:
        logger.exception("Create user failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc


@router.post("/advertisements", response_model=Advertisement)
async def create_advertisement(advertisement: AdvertisementCreate) -> Advertisement:
    '''
    Ручка для создания объявления
    '''
    try:
        return await advertisement_repo.create(
            seller_id=advertisement.seller_id,
            item_id=advertisement.item_id,
            name=advertisement.name,
            description=advertisement.description,
            category=advertisement.category,
            images_qty=advertisement.images_qty,
        )
    except SellerNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Seller not found") from exc
    except AdvertisementAlreadyExistsError as exc:
        raise HTTPException(status_code=409, detail="Advertisement already exists") from exc
    except StorageUnavailableError as exc:
        logger.exception("Create advertisement failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc


@router.post("/close")
async def close_advertisement(payload: CloseAdvertisementRequest) -> dict:
    '''
    Ручка для закрытия объявления
    '''
    try:
        close_result = await advertisement_repo.close(payload.item_id)
    except StorageUnavailableError as exc:
        logger.exception("Close advertisement failed item_id=%s", payload.item_id)
        raise HTTPException(status_code=500, detail="Internal server error") from exc

    if close_result is None:
        raise HTTPException(status_code=404, detail="Advertisement not found")

    try:
        await prediction_cache_storage.delete(payload.item_id)
    except Exception:
        logger.exception("Failed to delete prediction cache item_id=%s", payload.item_id)

    for moderation_result_id in close_result.moderation_result_ids:
        try:
            await moderation_result_cache_storage.delete(moderation_result_id)
        except Exception:
            logger.exception(
                "Failed to delete moderation_result cache task_id=%s item_id=%s",
                moderation_result_id,
                payload.item_id,
            )

    return {
        "item_id": close_result.item_id,
        "status": "closed",
        "message": "Advertisement closed",
    }
