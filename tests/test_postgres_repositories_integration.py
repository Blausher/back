import os
import time

import asyncpg
import pytest

from app.repositories.advertisements import AdvertisementRepository
from app.repositories.moderation_results import ModerationResultRepository
from app.repositories.processed_events import ProcessedEventRepository
from app.repositories.users import UserRepository


def _configure_default_pg_env() -> None:
    """Подставляет дефолтные параметры подключения к PostgreSQL для локального compose."""
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "15432")
    os.environ.setdefault("POSTGRES_USER", "blausher")
    os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
    os.environ.setdefault("POSTGRES_DB", "back")


def _pg_dsn() -> str:
    """Собирает DSN из переменных окружения PostgreSQL."""
    return (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )


async def _require_live_postgres() -> None:
    """Проверяет доступность PostgreSQL или пропускает integration-тест."""
    _configure_default_pg_env()
    try:
        connection = await asyncpg.connect(_pg_dsn())
    except Exception as exc:  # pragma: no cover - depends on runtime env
        pytest.skip(f"PostgreSQL is unavailable for integration test: {exc}")
    try:
        await connection.execute("SELECT 1")
    finally:
        await connection.close()


async def _cleanup(item_id: int, user_id: int) -> None:
    """Удаляет тестовые данные из связанных таблиц."""
    connection = await asyncpg.connect(_pg_dsn())
    try:
        await connection.execute("DELETE FROM processed_events WHERE item_id = $1", item_id)
        await connection.execute("DELETE FROM moderation_results WHERE item_id = $1", item_id)
        await connection.execute("DELETE FROM advertisements WHERE item_id = $1", item_id)
        await connection.execute("DELETE FROM users WHERE id = $1", user_id)
    finally:
        await connection.close()


def _new_ids() -> tuple[int, int]:
    """Генерирует валидные int32 id для тестовых записей."""
    suffix = int(time.time_ns() % 100_000_000)
    user_id = 1_000_000_000 + suffix
    item_id = 1_500_000_000 + suffix
    return user_id, item_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_user_and_advertisement_repositories():
    """Проверяет create/select в PostgreSQL через user и advertisement репозитории."""
    await _require_live_postgres()
    user_id, item_id = _new_ids()
    user_repo = UserRepository()
    advertisement_repo = AdvertisementRepository()

    await _cleanup(item_id, user_id)
    try:
        user = await user_repo.create(user_id=user_id, is_verified_seller=True)
        ad = await advertisement_repo.create(
            seller_id=user_id,
            item_id=item_id,
            name="Integration ad",
            description="Repository integration test advertisement",
            category=7,
            images_qty=2,
        )
        loaded = await advertisement_repo.select_advert(item_id)

        assert user.id == user_id
        assert ad.item_id == item_id
        assert loaded is not None
        assert loaded.item_id == item_id
        assert loaded.seller_id == user_id
    finally:
        await _cleanup(item_id, user_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_moderation_result_repository_create_pending_is_deduplicated():
    """Проверяет, что create_pending возвращает одну и ту же pending-задачу."""
    await _require_live_postgres()
    user_id, item_id = _new_ids()
    user_repo = UserRepository()
    advertisement_repo = AdvertisementRepository()
    moderation_repo = ModerationResultRepository()

    await _cleanup(item_id, user_id)
    try:
        await user_repo.create(user_id=user_id, is_verified_seller=False)
        await advertisement_repo.create(
            seller_id=user_id,
            item_id=item_id,
            name="Integration ad",
            description="Moderation integration check",
            category=4,
            images_qty=1,
        )

        first = await moderation_repo.create_pending(item_id)
        second = await moderation_repo.create_pending(item_id)
        loaded = await moderation_repo.get_by_id(first.id)

        assert second.id == first.id
        assert first.status == "pending"
        assert loaded is not None
        assert loaded.item_id == item_id
        assert loaded.status == "pending"
    finally:
        await _cleanup(item_id, user_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_advertisement_close_removes_advertisement_and_moderation_results():
    """Проверяет hard delete объявления и связанных moderation results."""
    await _require_live_postgres()
    user_id, item_id = _new_ids()
    user_repo = UserRepository()
    advertisement_repo = AdvertisementRepository()
    moderation_repo = ModerationResultRepository()

    await _cleanup(item_id, user_id)
    try:
        await user_repo.create(user_id=user_id, is_verified_seller=True)
        await advertisement_repo.create(
            seller_id=user_id,
            item_id=item_id,
            name="Integration ad",
            description="Close integration check",
            category=3,
            images_qty=3,
        )
        pending = await moderation_repo.create_pending(item_id)

        close_result = await advertisement_repo.close(item_id)

        assert close_result is not None
        assert close_result.item_id == item_id
        assert pending.id in close_result.moderation_result_ids
        assert await advertisement_repo.select_advert(item_id) is None
        assert await moderation_repo.get_by_id(pending.id) is None
        assert await advertisement_repo.close(item_id) is None
    finally:
        await _cleanup(item_id, user_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_moderation_result_worker_methods_update_pending_task():
    """Проверяет worker-ориентированные методы moderation result репозитория."""
    await _require_live_postgres()
    user_id, item_id = _new_ids()
    user_repo = UserRepository()
    advertisement_repo = AdvertisementRepository()
    moderation_repo = ModerationResultRepository()

    await _cleanup(item_id, user_id)
    try:
        await user_repo.create(user_id=user_id, is_verified_seller=True)
        await advertisement_repo.create(
            seller_id=user_id,
            item_id=item_id,
            name="Integration ad",
            description="Worker repository methods",
            category=6,
            images_qty=2,
        )

        pending = await moderation_repo.create_pending(item_id)
        pending_task_id = await moderation_repo.get_pending_task_id(item_id)
        completed_task_id = await moderation_repo.mark_completed(item_id, True, 0.87)
        loaded = await moderation_repo.get_by_id(pending.id)

        assert pending_task_id == pending.id
        assert completed_task_id == pending.id
        assert loaded is not None
        assert loaded.status == "completed"
        assert loaded.is_violation is True
        assert loaded.probability == pytest.approx(0.87)
        assert await moderation_repo.get_pending_task_id(item_id) is None
    finally:
        await _cleanup(item_id, user_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_moderation_result_mark_failed_and_processed_event_idempotency():
    """Проверяет failed-обновление и идемпотентность processed_events."""
    await _require_live_postgres()
    user_id, item_id = _new_ids()
    user_repo = UserRepository()
    advertisement_repo = AdvertisementRepository()
    moderation_repo = ModerationResultRepository()
    processed_event_repo = ProcessedEventRepository()

    await _cleanup(item_id, user_id)
    try:
        await user_repo.create(user_id=user_id, is_verified_seller=False)
        await advertisement_repo.create(
            seller_id=user_id,
            item_id=item_id,
            name="Integration ad",
            description="Processed events integration",
            category=8,
            images_qty=1,
        )

        pending = await moderation_repo.create_pending(item_id)
        failed_task_id = await moderation_repo.mark_failed(item_id, "temporary error")
        failed = await moderation_repo.get_by_id(pending.id)
        retried_pending = await moderation_repo.create_pending(item_id)

        first_insert = await processed_event_repo.register_processing(
            event_id=f"moderation:{item_id}:{retried_pending.id}",
            item_id=item_id,
            moderation_result_id=retried_pending.id,
        )
        second_insert = await processed_event_repo.register_processing(
            event_id=f"moderation:{item_id}:{retried_pending.id}",
            item_id=item_id,
            moderation_result_id=retried_pending.id,
        )

        assert failed_task_id == pending.id
        assert failed is not None
        assert failed.status == "failed"
        assert failed.error_message == "temporary error"
        assert retried_pending.id != pending.id
        assert first_insert is True
        assert second_insert is False
    finally:
        await _cleanup(item_id, user_id)
