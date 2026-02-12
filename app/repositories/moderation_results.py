from dataclasses import dataclass
from typing import Any, Mapping

from app.clients.postgres import get_pg_connection
from app.errors import StorageUnavailableError
from app.models.moderation_result import ModerationResult


@dataclass(frozen=True)
class ModerationResultStorage:
    async def create_pending(self, item_id: int) -> Mapping[str, Any]:
        get_existing_query = """
            SELECT id, item_id, status, is_violation, probability, error_message, created_at, processed_at
            FROM moderation_results
            WHERE item_id = $1 AND status IN ('pending', 'completed')
            ORDER BY CASE WHEN status = 'pending' THEN 0 ELSE 1 END, id DESC
            LIMIT 1
        """
        insert_query = """
            INSERT INTO moderation_results (item_id, status)
            VALUES ($1, 'pending')
            ON CONFLICT (item_id) WHERE status = 'pending' DO NOTHING
            RETURNING id, item_id, status, is_violation, probability, error_message, created_at, processed_at
        """
        try:
            async with get_pg_connection() as connection:
                existing = await connection.fetchrow(get_existing_query, item_id)
                if existing is not None:
                    return dict(existing)

                # Без явной сериализации: дедупликацию обеспечивает partial unique index.
                # Редкая гонка: конфликт случился, но запись уже успела сменить статус;
                # тогда повторяем попытку вставки.
                for _ in range(2):
                    record = await connection.fetchrow(insert_query, item_id)
                    if record is not None:
                        return dict(record)
                    existing = await connection.fetchrow(get_existing_query, item_id)
                    if existing is not None:
                        return dict(existing)
                raise StorageUnavailableError("Failed to create pending moderation result")
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc

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
        raw_result = await self.moderation_result_storage.create_pending(item_id=item_id)
        return ModerationResult.model_validate(raw_result)

    async def get_by_id(self, moderation_result_id: int) -> ModerationResult | None:
        raw_result = await self.moderation_result_storage.get_by_id(moderation_result_id)
        if raw_result is None:
            return None
        return ModerationResult.model_validate(raw_result)
