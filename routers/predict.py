import logging

import numpy as np
from fastapi import APIRouter, HTTPException, Request

from models.advertisement import Advertisement
from repositories.advertisements import AdvertisementRepository
from services import moderation

router = APIRouter()
logger = logging.getLogger(__name__)
advertisement_repo = AdvertisementRepository()


@router.post("/predict")
async def predict(advertisement: Advertisement, request: Request) -> dict:
    """
    Возвращает валидность объявления и вероятность.
    """
    model = _get_model(request)
    is_valid, probability = _predict(advertisement, model)

    logger.info(
        "Predict result seller_id=%s item_id=%s is_valid=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        is_valid,
        probability,
    )

    return {"is_valid": is_valid, "probability": probability}


@router.get("/simple_predict")
async def simple_predict(item_id: int, request: Request) -> dict:
    """
    Возвращает валидность объявления по item_id.
    """
    model = _get_model(request)

    try:
        advertisement = await advertisement_repo.select_advert(item_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database is not available") from exc
    if advertisement is None:
        raise HTTPException(status_code=404, detail="Advertisement not found")

    is_valid, probability = _predict(advertisement, model)

    logger.info(
        "Simple predict result seller_id=%s item_id=%s is_valid=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        is_valid,
        probability,
    )

    return {"is_valid": is_valid, "probability": probability}


def _get_model(request: Request):
    model = getattr(request.app.state, "model", None)
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded")
    return model


def _predict(advertisement: Advertisement, model) -> tuple[bool, float]:
    features = np.array(
        [[
            1.0 if advertisement.is_verified_seller else 0.0,
            min(advertisement.images_qty, 10) / 10.0,
            len(advertisement.description) / 1000.0,
            advertisement.category / 100.0,
        ]],
        dtype=float,
    )

    logger.info(
        "Predict request seller_id=%s item_id=%s features=%s",
        advertisement.seller_id,
        advertisement.item_id,
        features.tolist(),
    )

    try:
        probability = float(model.predict_proba(features)[0][1])
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Model inference failed",
        ) from exc

    try:
        is_valid = moderation.predict_has_violations(advertisement)
    except moderation.BusinessLogicError as exc:
        raise HTTPException(
            status_code=500,
            detail="Business logic prediction failed",
        ) from exc

    return is_valid, probability
