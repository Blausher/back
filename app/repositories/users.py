from dataclasses import dataclass
from typing import Any, Mapping

from clients.postgres import get_pg_connection
from models.user import User


@dataclass(frozen=True)
class UserStorage:
    async def create(self, user_id: int, is_verified_seller: bool) -> Mapping[str, Any]:
        query = """
            INSERT INTO users (id, is_verified_seller)
            VALUES ($1, $2)
            RETURNING *
        """
        async with get_pg_connection() as connection:
            record = await connection.fetchrow(query, user_id, is_verified_seller)

        return dict(record)


@dataclass(frozen=True)
class UserRepository:
    user_storage: UserStorage = UserStorage()

    async def create(self, user_id: int, is_verified_seller: bool) -> User:
        raw_user = await self.user_storage.create(
            user_id=user_id,
            is_verified_seller=is_verified_seller,
        )
        return User.model_validate(raw_user)
