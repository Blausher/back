from dataclasses import dataclass
from typing import Any, Mapping

from clients.postgres import get_pg_connection
from models.advertisement import Advertisement


@dataclass(frozen=True)
class AdvertisementStorage:
    async def get_with_user(self, item_id: int) -> Mapping[str, Any] | None:
        query = """
            SELECT
                a.item_id,
                a.seller_id,
                a.name,
                a.description,
                a.category,
                a.images_qty,
                u.is_verified_seller
            FROM advertisements AS a
            JOIN users AS u ON u.id = a.seller_id
            WHERE a.item_id = $1
        """
        async with get_pg_connection() as connection:
            record = await connection.fetchrow(query, item_id)
        if record is None:
            return None
        return dict(record)


@dataclass(frozen=True)
class AdvertisementRepository:
    advertisement_storage: AdvertisementStorage = AdvertisementStorage()

    async def get_with_user(self, item_id: int) -> Advertisement | None:
        raw_ad = await self.advertisement_storage.get_with_user(item_id)
        if raw_ad is None:
            return None
        return Advertisement.model_validate(raw_ad)
