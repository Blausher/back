from fastapi import APIRouter, HTTPException

from models.advertisement import Advertisement
from services import moderation

router = APIRouter()


@router.post("/predict", response_model=bool)
async def predict(advertisement: Advertisement) -> bool:
    '''
    Содержит ли объявление нарушения
    '''
    try:
        return moderation.predict_has_violations(advertisement)
    except moderation.BusinessLogicError as exc:
        raise HTTPException(status_code=500, detail="Business logic error") from exc
