import logging

import numpy as np
from fastapi import APIRouter, HTTPException, Request

from models.advertisement import Advertisement
from services import moderation

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/predict")
async def predict(advertisement: Advertisement, request: Request) -> dict:
    """
    Возвращает факт нарушения и вероятность.
    """
    model = getattr(request.app.state, "model", None)
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded")

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
        is_violation = moderation.predict_has_violations(advertisement)
    except moderation.BusinessLogicError as exc:
        raise HTTPException(
            status_code=500,
            detail="Business logic prediction failed",
        ) from exc

    logger.info(
        "Predict result seller_id=%s item_id=%s is_violation=%s probability=%s",
        advertisement.seller_id,
        advertisement.item_id,
        is_violation,
        probability,
    )

    return {"is_violation": is_violation, "probability": probability}
