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
from app.models.user import User
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.users import UserRepository

router = APIRouter()
logger = logging.getLogger(__name__)
advertisement_repo = AdvertisementRepository()
user_repo = UserRepository()


@router.post("/users", response_model=User)
async def create_user(user: User) -> User:
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
