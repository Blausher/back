from dataclasses import dataclass

from app.clients.postgres import get_pg_connection
from app.errors import StorageUnavailableError


@dataclass(frozen=True)
class ProcessedEventStorage:
    async def register_processing(
        self,
        event_id: str,
        item_id: int,
        moderation_result_id: int,
    ) -> bool:
        query = """
            INSERT INTO processed_events (event_id, item_id, moderation_result_id)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
        """
        try:
            async with get_pg_connection() as connection:
                async with connection.transaction():
                    result = await connection.execute(
                        query,
                        event_id,
                        item_id,
                        moderation_result_id,
                    )
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        return not result.endswith("0")


@dataclass(frozen=True)
class ProcessedEventRepository:
    processed_event_storage: ProcessedEventStorage = ProcessedEventStorage()

    async def register_processing(
        self,
        event_id: str,
        item_id: int,
        moderation_result_id: int,
    ) -> bool:
        return await self.processed_event_storage.register_processing(
            event_id=event_id,
            item_id=item_id,
            moderation_result_id=moderation_result_id,
        )
