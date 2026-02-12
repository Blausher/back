from pydantic import BaseModel, Field


class AsyncPredictRequest(BaseModel):
    item_id: int = Field(ge=0)


class AsyncPredictResponse(BaseModel):
    task_id: int = Field(ge=0)
    status: str = Field(min_length=1)
    message: str = Field(min_length=1)


class ModerationResultResponse(BaseModel):
    task_id: int = Field(ge=0)
    status: str = Field(min_length=1)
    is_violation: bool | None = None
    probability: float | None = None
