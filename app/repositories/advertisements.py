from dataclasses import dataclass
from typing import Any, Mapping

from asyncpg import exceptions as pg_exc

from app.clients.postgres import get_pg_connection
from app.errors import (
    AdvertisementAlreadyExistsError,
    SellerNotFoundError,
    StorageUnavailableError,
)
from app.models.advertisement import Advertisement


@dataclass(frozen=True)
class AdvertisementStorage:
    async def select_advert(self, item_id: int) -> Mapping[str, Any] | None:
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
        try:
            async with get_pg_connection() as connection:
                record = await connection.fetchrow(query, item_id)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        if record is None:
            return None
        return dict(record)

    async def create(
        self,
        seller_id: int,
        item_id: int,
        name: str,
        description: str,
        category: int,
        images_qty: int,
    ) -> Mapping[str, Any]:
        insert_query = """
            INSERT INTO advertisements (item_id, seller_id, name, description, category, images_qty)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING item_id, seller_id, name, description, category, images_qty
        """
        seller_exists_query = """
            SELECT id
            FROM users
            WHERE id = $1
        """

        try:
            async with get_pg_connection() as connection:
                seller_row = await connection.fetchrow(seller_exists_query, seller_id)
                if seller_row is None:
                    raise SellerNotFoundError("Seller not found")
                record = await connection.fetchrow(
                    insert_query,
                    item_id,
                    seller_id,
                    name,
                    description,
                    category,
                    images_qty,
                )
        except SellerNotFoundError:
            raise
        except pg_exc.UniqueViolationError as exc:
            raise AdvertisementAlreadyExistsError("Advertisement already exists") from exc
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc

        return dict(record)

    async def close(self, item_id: int) -> Mapping[str, Any] | None:
        query = """
            WITH deleted_results AS (
                DELETE FROM moderation_results
                WHERE item_id = $1
                RETURNING id
            ),
            deleted_advertisement AS (
                DELETE FROM advertisements
                WHERE item_id = $1
                RETURNING item_id
            )
            SELECT
                deleted_advertisement.item_id AS item_id,
                COALESCE(
                    (SELECT array_agg(id ORDER BY id) FROM deleted_results),
                    ARRAY[]::INTEGER[]
                ) AS moderation_result_ids
            FROM deleted_advertisement
        """
        try:
            async with get_pg_connection() as connection:
                async with connection.transaction():
                    record = await connection.fetchrow(query, item_id)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        if record is None:
            return None
        return dict(record)


@dataclass(frozen=True)
class AdvertisementCloseResult:
    item_id: int
    moderation_result_ids: list[int]


@dataclass(frozen=True)
class AdvertisementRepository:
    advertisement_storage: AdvertisementStorage = AdvertisementStorage()

    async def select_advert(self, item_id: int) -> Advertisement | None:
        raw_ad = await self.advertisement_storage.select_advert(item_id)
        if raw_ad is None:
            return None
        return Advertisement.model_validate(raw_ad)

    async def create(
        self,
        seller_id: int,
        item_id: int,
        name: str,
        description: str,
        category: int,
        images_qty: int,
    ) -> Advertisement:
        await self.advertisement_storage.create(
            seller_id=seller_id,
            item_id=item_id,
            name=name,
            description=description,
            category=category,
            images_qty=images_qty,
        )
        raw_ad = await self.advertisement_storage.select_advert(item_id)
        if raw_ad is None:
            raise StorageUnavailableError("Storage operation failed")
        return Advertisement.model_validate(raw_ad)

    async def close(self, item_id: int) -> AdvertisementCloseResult | None:
        raw_result = await self.advertisement_storage.close(item_id)
        if raw_result is None:
            return None
        moderation_result_ids = [int(value) for value in raw_result["moderation_result_ids"]]
        return AdvertisementCloseResult(
            item_id=int(raw_result["item_id"]),
            moderation_result_ids=moderation_result_ids,
        )
