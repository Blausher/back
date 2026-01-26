import numpy as np
from fastapi import APIRouter, HTTPException, Request

from models.advertisement import Advertisement
from services import moderation

router = APIRouter()


@router.post("/predict")
async def predict(advertisement: Advertisement, request: Request) -> dict:
    """
    Возвращает факт нарушения и вероятность.
    """
    model = getattr(request.app.state, "model", None)
    if model is None:
        raise HTTPException(status_code=500, detail="Model is not loaded")

    features = np.array([[1.0 if advertisement.is_verified_seller else 0.0,
                        min(advertisement.images_qty, 10) / 10.0,
                        len(advertisement.description) / 1000.0,
                        advertisement.category / 100.0]],
                        dtype=float)

    try:
        probability = float(model.predict_proba(features)[0][1])
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Model inference error") from exc
    
    try:
        is_violation = moderation.predict_has_violations(advertisement)
    except moderation.BusinessLogicError as exc:
        raise HTTPException(status_code=500, detail="Business logic error") from exc

    return {"is_violation": is_violation, "probability": probability}


