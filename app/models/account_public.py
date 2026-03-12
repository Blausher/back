from pydantic import BaseModel, Field


class AccountPublic(BaseModel):
    id: int = Field(ge=1)
    login: str = Field(min_length=1)
    is_blocked: bool = False
