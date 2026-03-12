from pydantic import BaseModel, Field


class AccountCreateRequest(BaseModel):
    login: str = Field(min_length=1)
    password: str = Field(min_length=1)
