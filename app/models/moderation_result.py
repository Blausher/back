from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ModerationResult(BaseModel):
    id: int = Field(ge=0)
    item_id: int = Field(ge=0)
    status: str = Field(min_length=1)
    is_violation: Optional[bool] = None
    probability: Optional[float] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
