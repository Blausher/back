from datetime import datetime

from pydantic import BaseModel, Field


class AccessTokenPayload(BaseModel):
    account_id: int = Field(ge=1)
    login: str = Field(min_length=1)
    issued_at: datetime
    expires_at: datetime
