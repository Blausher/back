from dataclasses import dataclass
from datetime import timedelta
from json import dumps, loads
from typing import Any, Mapping

from app.clients.redis import get_redis_connection


@dataclass(frozen=True)
class PredictionRedisStorage:
    # Предсказание модели стабильно для конкретной версии объявления и не требует
    # пересчета на каждом запросе, поэтому держим его в кэше сутки, чтобы снять
    # нагрузку с модели и базы. Более длинный TTL повышает риск отдавать устаревший
    # результат после изменений объявления
    _TTL: timedelta = timedelta(days=1)
    _KEY_PREFIX: str = "prediction"

    async def set(self, row_id: int, row: Mapping[str, Any]) -> None:
        key = self._build_key(row_id)
        async with get_redis_connection() as connection:
            pipeline = connection.pipeline()
            pipeline.set(name=key, value=dumps(dict(row)))
            pipeline.expire(key, int(self._TTL.total_seconds()))
            await pipeline.execute()

    async def get(self, row_id: int) -> Mapping[str, Any] | None:
        key = self._build_key(row_id)
        async with get_redis_connection() as connection:
            row = await connection.get(key)
            if row:
                return loads(row)
            return None

    async def delete(self, row_id: int) -> None:
        key = self._build_key(row_id)
        async with get_redis_connection() as connection:
            await connection.delete(key)

    def _build_key(self, row_id: int) -> str:
        return f"{self._KEY_PREFIX}:{row_id}"


@dataclass(frozen=True)
class ModerationResultRedisStorage:
    # Pending-статус нужен только как короткий буфер между созданием задачи и
    # ответом воркера, поэтому 15 секунд хватит, чтобы повторные запросы
    # не плодили дубли. 
    _PENDING_TTL: timedelta = timedelta(seconds=15)
    # Terminal-результаты можно держать сутки по той же причине, что и prediction:
    # они читаются чаще, чем меняются, и суточный TTL снижает число обращений к БД
    # без заметного риска долго показывать неактуальные данные.
    _TERMINAL_TTL: timedelta = timedelta(days=1)
    _KEY_PREFIX: str = "moderation_result"

    async def set(self, row_id: int, row: Mapping[str, Any]) -> None:
        key = self._build_key(row_id)
        ttl = self._ttl_for_status(str(row.get("status")))
        async with get_redis_connection() as connection:
            pipeline = connection.pipeline()
            pipeline.set(name=key, value=dumps(dict(row)))
            pipeline.expire(key, int(ttl.total_seconds()))
            await pipeline.execute()

    async def get(self, row_id: int) -> Mapping[str, Any] | None:
        key = self._build_key(row_id)
        async with get_redis_connection() as connection:
            row = await connection.get(key)
            if row:
                return loads(row)
            return None

    async def delete(self, row_id: int) -> None:
        key = self._build_key(row_id)
        async with get_redis_connection() as connection:
            await connection.delete(key)

    def _build_key(self, row_id: int) -> str:
        return f"{self._KEY_PREFIX}:{row_id}"

    def _ttl_for_status(self, status: str) -> timedelta:
        if status in {"completed", "failed"}:
            return self._TERMINAL_TTL
        return self._PENDING_TTL
