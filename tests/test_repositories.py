from contextlib import asynccontextmanager

import pytest

from app.errors import SellerNotFoundError, StorageUnavailableError
from app.models.advertisement import Advertisement
from app.models.moderation_result import ModerationResult
from app.models.user import User
from app.repositories import advertisements as ads_repo
from app.repositories import moderation_results as mr_repo
from app.repositories import processed_events as pe_repo
from app.repositories import users as users_repo


class DummyTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyConnection:
    def __init__(self, row, execute_result="INSERT 0 1"):
        self.row = row
        self.execute_result = execute_result
        self.executed = []
        self.fetched = []

    def transaction(self):
        return DummyTransaction()

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return self.execute_result

    async def fetchrow(self, query, *args):
        self.fetched.append((query, args))
        return self.row


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class SequencedConnection(DummyConnection):
    def __init__(self, rows):
        super().__init__(row=None)
        self.rows = list(rows)

    async def fetchrow(self, query, *args):
        self.fetched.append((query, args))
        if not self.rows:
            return None
        return self.rows.pop(0)

    def transaction(self):
        return DummyTx()


@pytest.mark.asyncio
async def test_user_repository_create(monkeypatch):
    """Создает пользователя через репозиторий и возвращает модель."""
    expected = {"id": 7, "is_verified_seller": True}
    connection = DummyConnection(expected)

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(users_repo, "get_pg_connection", conn_stub)

    repo = users_repo.UserRepository()
    user = await repo.create(user_id=7, is_verified_seller=True)

    assert isinstance(user, User)
    assert user.id == 7
    assert user.is_verified_seller is True


@pytest.mark.asyncio
async def test_advertisement_repository_create(monkeypatch):
    """Создает объявление через репозиторий и возвращает модель."""
    expected = {
        "seller_id": 7,
        "is_verified_seller": True,
        "item_id": 10,
        "name": "Desk",
        "description": "Wooden desk",
        "category": 2,
        "images_qty": 1,
    }
    connection = DummyConnection(expected)

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    ad = await repo.create(
        seller_id=expected["seller_id"],
        item_id=expected["item_id"],
        name=expected["name"],
        description=expected["description"],
        category=expected["category"],
        images_qty=expected["images_qty"],
    )

    assert isinstance(ad, Advertisement)
    assert ad.item_id == expected["item_id"]
    assert ad.seller_id == expected["seller_id"]
    assert ad.is_verified_seller is True

    assert connection.fetched


@pytest.mark.asyncio
async def test_advertisement_repository_create_raises_when_seller_missing(monkeypatch):
    """Возвращает доменную ошибку, если продавец не найден до INSERT."""
    connection = SequencedConnection(rows=[None])

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()

    with pytest.raises(SellerNotFoundError):
        await repo.create(
            seller_id=999,
            item_id=10,
            name="Desk",
            description="Wooden desk",
            category=2,
            images_qty=1,
        )


@pytest.mark.asyncio
async def test_moderation_result_create_pending_returns_existing(monkeypatch):
    """Возвращает существующую pending-задачу и не создает дубль."""
    existing = {
        "id": 321,
        "item_id": 42,
        "status": "pending",
        "is_violation": None,
        "probability": None,
        "error_message": None,
        "created_at": None,
        "processed_at": None,
    }
    # Первая fetchrow (SELECT pending/completed) -> existing
    connection = SequencedConnection(rows=[existing])

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    result = await repo.create_pending(42)

    assert isinstance(result, ModerationResult)
    assert result.id == 321
    assert result.item_id == 42
    assert result.status == "pending"
    assert len(connection.fetched) == 1
    assert "SELECT id, item_id, status" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_moderation_result_create_pending_returns_existing_completed(monkeypatch):
    """Возвращает существующую completed-задачу и не создает дубль."""
    existing = {
        "id": 322,
        "item_id": 42,
        "status": "completed",
        "is_violation": True,
        "probability": 0.91,
        "error_message": None,
        "created_at": None,
        "processed_at": None,
    }
    connection = SequencedConnection(rows=[existing])

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    result = await repo.create_pending(42)

    assert isinstance(result, ModerationResult)
    assert result.id == 322
    assert result.item_id == 42
    assert result.status == "completed"
    assert len(connection.fetched) == 1
    assert "SELECT id, item_id, status" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_moderation_result_get_pending_task_id(monkeypatch):
    connection = DummyConnection({"id": 777})

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    pending_task_id = await repo.get_pending_task_id(42)

    assert pending_task_id == 777
    assert len(connection.fetched) == 1
    assert "status = 'pending'" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_moderation_result_mark_completed(monkeypatch):
    connection = DummyConnection({"id": 778})

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    task_id = await repo.mark_completed(42, True, 0.95)

    assert task_id == 778
    assert len(connection.fetched) == 1
    assert "status = 'completed'" in connection.fetched[0][0]
    assert connection.fetched[0][1] == (42, True, 0.95)


@pytest.mark.asyncio
async def test_moderation_result_mark_failed_truncates_error(monkeypatch):
    connection = DummyConnection({"id": 779})

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    error_message = "x" * 1500
    task_id = await repo.mark_failed(42, error_message)

    assert task_id == 779
    assert len(connection.fetched) == 1
    assert "status = 'failed'" in connection.fetched[0][0]
    assert connection.fetched[0][1][0] == 42
    assert len(connection.fetched[0][1][1]) == 1000


@pytest.mark.asyncio
async def test_processed_event_repository_register_processing_first_insert(monkeypatch):
    connection = DummyConnection(row=None, execute_result="INSERT 0 1")

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(pe_repo, "get_pg_connection", conn_stub)

    repo = pe_repo.ProcessedEventRepository()
    first_time = await repo.register_processing(
        event_id="moderation:42:10",
        item_id=42,
        moderation_result_id=10,
    )

    assert first_time is True
    assert len(connection.executed) == 1
    assert "INSERT INTO processed_events" in connection.executed[0][0]


@pytest.mark.asyncio
async def test_processed_event_repository_register_processing_duplicate(monkeypatch):
    connection = DummyConnection(row=None, execute_result="INSERT 0 0")

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(pe_repo, "get_pg_connection", conn_stub)

    repo = pe_repo.ProcessedEventRepository()
    first_time = await repo.register_processing(
        event_id="moderation:42:10",
        item_id=42,
        moderation_result_id=10,
    )

    assert first_time is False
    assert len(connection.executed) == 1


@pytest.mark.asyncio
async def test_advertisement_repository_close_success(monkeypatch):
    expected = {
        "item_id": 10,
        "moderation_result_ids": [501, 502],
    }
    connection = DummyConnection(expected)

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    result = await repo.close(item_id=10)

    assert result is not None
    assert result.item_id == 10
    assert result.moderation_result_ids == [501, 502]
    assert len(connection.fetched) == 1
    assert "DELETE FROM moderation_results" in connection.fetched[0][0]
    assert "DELETE FROM advertisements" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_advertisement_repository_close_not_found(monkeypatch):
    connection = DummyConnection(None)

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    result = await repo.close(item_id=404)

    assert result is None
    assert len(connection.fetched) == 1


@pytest.mark.asyncio
async def test_advertisement_repository_close_raises_storage_unavailable(monkeypatch):
    @asynccontextmanager
    async def conn_stub():
        raise RuntimeError("db unavailable")
        yield

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    with pytest.raises(StorageUnavailableError):
        await repo.close(item_id=10)
