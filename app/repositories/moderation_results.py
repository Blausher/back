from dataclasses import dataclass
from typing import Any, Mapping

from app.clients.postgres import get_pg_connection
from app.errors import StorageUnavailableError
from app.models.moderation_result import ModerationResult


@dataclass(frozen=True)
class ModerationResultStorage:
    async def create(self, item_id: int, status: str) -> Mapping[str, Any]:
        query = """
            INSERT INTO moderation_results (item_id, status)
            VALUES ($1, $2)
            RETURNING id, item_id, status, is_violation, probability, error_message, created_at, processed_at
        """
        try:
            async with get_pg_connection() as connection:
                record = await connection.fetchrow(query, item_id, status)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        return dict(record)

    async def get_by_id(self, moderation_result_id: int) -> Mapping[str, Any] | None:
        query = """
            SELECT id, item_id, status, is_violation, probability, error_message, created_at, processed_at
            FROM moderation_results
            WHERE id = $1
        """
        try:
            async with get_pg_connection() as connection:
                record = await connection.fetchrow(query, moderation_result_id)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        if record is None:
            return None
        return dict(record)


@dataclass(frozen=True)
class ModerationResultRepository:
    moderation_result_storage: ModerationResultStorage = ModerationResultStorage()

    async def create_pending(self, item_id: int) -> ModerationResult:
        raw_result = await self.moderation_result_storage.create(
            item_id=item_id,
            status="pending",
        )
        return ModerationResult.model_validate(raw_result)

    async def get_by_id(self, moderation_result_id: int) -> ModerationResult | None:
        raw_result = await self.moderation_result_storage.get_by_id(moderation_result_id)
        if raw_result is None:
            return None
        return ModerationResult.model_validate(raw_result)
