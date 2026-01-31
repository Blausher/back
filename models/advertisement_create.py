from pydantic import BaseModel, Field


class AdvertisementCreate(BaseModel):
    seller_id: int = Field(ge=0)
    item_id: int = Field(ge=0)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    category: int
    images_qty: int = Field(ge=0)
