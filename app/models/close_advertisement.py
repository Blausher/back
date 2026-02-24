from pydantic import BaseModel, Field


class CloseAdvertisementRequest(BaseModel):
    item_id: int = Field(ge=0)
