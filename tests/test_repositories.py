from contextlib import asynccontextmanager
import hashlib

import pytest

from app.errors import SellerNotFoundError, StorageUnavailableError
from app.models.advertisement import Advertisement
from app.models.moderation_result import ModerationResult
from app.models.user import User
from app.repositories import advertisements as ads_repo
from app.repositories import accounts as accounts_repo
from app.repositories import moderation_results as mr_repo
from app.repositories import processed_events as pe_repo
from app.repositories import users as users_repo
from tests.id_factory import new_id


class DummyAccountStorage:
    def __init__(self, row):
        self.row = row
        self.create_calls: list[tuple[str, str]] = []
        self.lookup_calls: list[tuple[str, str]] = []

    async def create(self, login: str, password_hash: str):
        self.create_calls.append((login, password_hash))
        return self.row

    async def get_by_login_and_password_hash(self, login: str, password_hash: str):
        self.lookup_calls.append((login, password_hash))
        return self.row


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
    user_id = new_id()
    expected = {"id": user_id, "is_verified_seller": True}
    connection = DummyConnection(expected)

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(users_repo, "get_pg_connection", conn_stub)

    repo = users_repo.UserRepository()
    user = await repo.create(user_id=user_id, is_verified_seller=True)

    assert isinstance(user, User)
    assert user.id == user_id
    assert user.is_verified_seller is True


@pytest.mark.asyncio
async def test_account_repository_create_hashes_password_before_storage():
    password = "s3cret-password"
    expected_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    account_id = new_id()
    storage = DummyAccountStorage(
        {"id": account_id, "login": "tester", "password": expected_hash, "is_blocked": False}
    )

    repo = accounts_repo.AccountRepository(account_storage=storage)
    account = await repo.create(login="tester", password=password)

    assert account.password == expected_hash
    assert storage.create_calls == [("tester", expected_hash)]


@pytest.mark.asyncio
async def test_account_repository_get_by_login_and_password_hashes_password_before_storage():
    password = "s3cret-password"
    expected_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
    account_id = new_id()
    storage = DummyAccountStorage(
        {"id": account_id, "login": "tester", "password": expected_hash, "is_blocked": False}
    )

    repo = accounts_repo.AccountRepository(account_storage=storage)
    account = await repo.get_by_login_and_password(login="tester", password=password)

    assert account is not None
    assert account.password == expected_hash
    assert storage.lookup_calls == [("tester", expected_hash)]


@pytest.mark.asyncio
async def test_advertisement_repository_create(monkeypatch):
    """Создает объявление через репозиторий и возвращает модель."""
    seller_id = new_id()
    item_id = new_id()
    expected = {
        "seller_id": seller_id,
        "is_verified_seller": True,
        "item_id": item_id,
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
    seller_id = new_id()
    item_id = new_id()

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()

    with pytest.raises(SellerNotFoundError):
        await repo.create(
            seller_id=seller_id,
            item_id=item_id,
            name="Desk",
            description="Wooden desk",
            category=2,
            images_qty=1,
        )


@pytest.mark.asyncio
async def test_moderation_result_create_pending_returns_existing(monkeypatch):
    """Возвращает существующую pending-задачу и не создает дубль."""
    moderation_result_id = new_id()
    item_id = new_id()
    existing = {
        "id": moderation_result_id,
        "item_id": item_id,
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
    result = await repo.create_pending(item_id)

    assert isinstance(result, ModerationResult)
    assert result.id == moderation_result_id
    assert result.item_id == item_id
    assert result.status == "pending"
    assert len(connection.fetched) == 1
    assert "SELECT id, item_id, status" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_moderation_result_create_pending_returns_existing_completed(monkeypatch):
    """Возвращает существующую completed-задачу и не создает дубль."""
    moderation_result_id = new_id()
    item_id = new_id()
    existing = {
        "id": moderation_result_id,
        "item_id": item_id,
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
    result = await repo.create_pending(item_id)

    assert isinstance(result, ModerationResult)
    assert result.id == moderation_result_id
    assert result.item_id == item_id
    assert result.status == "completed"
    assert len(connection.fetched) == 1
    assert "SELECT id, item_id, status" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_moderation_result_create_pending_reads_existing_after_conflict(monkeypatch):
    moderation_result_id = new_id()
    item_id = new_id()
    existing = {
        "id": moderation_result_id,
        "item_id": item_id,
        "status": "pending",
        "is_violation": None,
        "probability": None,
        "error_message": None,
        "created_at": None,
        "processed_at": None,
    }
    connection = SequencedConnection(rows=[None, None, existing])

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    result = await repo.create_pending(item_id)

    assert isinstance(result, ModerationResult)
    assert result.id == moderation_result_id
    assert result.status == "pending"
    assert len(connection.fetched) == 3


@pytest.mark.asyncio
async def test_moderation_result_get_pending_task_id(monkeypatch):
    item_id = new_id()
    pending_task_id = new_id()
    connection = DummyConnection({"id": pending_task_id})

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    found_task_id = await repo.get_pending_task_id(item_id)

    assert found_task_id == pending_task_id
    assert len(connection.fetched) == 1
    assert "status = 'pending'" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_moderation_result_mark_completed(monkeypatch):
    item_id = new_id()
    task_id = new_id()
    connection = DummyConnection({"id": task_id})

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    marked_task_id = await repo.mark_completed(item_id, True, 0.95)

    assert marked_task_id == task_id
    assert len(connection.fetched) == 1
    assert "status = 'completed'" in connection.fetched[0][0]
    assert connection.fetched[0][1] == (item_id, True, 0.95)


@pytest.mark.asyncio
async def test_moderation_result_mark_failed_truncates_error(monkeypatch):
    item_id = new_id()
    task_id = new_id()
    connection = DummyConnection({"id": task_id})

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(mr_repo, "get_pg_connection", conn_stub)

    repo = mr_repo.ModerationResultRepository()
    error_message = "x" * 1500
    marked_task_id = await repo.mark_failed(item_id, error_message)

    assert marked_task_id == task_id
    assert len(connection.fetched) == 1
    assert "status = 'failed'" in connection.fetched[0][0]
    assert connection.fetched[0][1][0] == item_id
    assert len(connection.fetched[0][1][1]) == 1000


@pytest.mark.asyncio
async def test_processed_event_repository_register_processing_first_insert(monkeypatch):
    connection = DummyConnection(row=None, execute_result="INSERT 0 1")
    item_id = new_id()
    moderation_result_id = new_id()

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(pe_repo, "get_pg_connection", conn_stub)

    repo = pe_repo.ProcessedEventRepository()
    first_time = await repo.register_processing(
        event_id=f"moderation:{item_id}:{moderation_result_id}",
        item_id=item_id,
        moderation_result_id=moderation_result_id,
    )

    assert first_time is True
    assert len(connection.executed) == 1
    assert "INSERT INTO processed_events" in connection.executed[0][0]


@pytest.mark.asyncio
async def test_processed_event_repository_register_processing_duplicate(monkeypatch):
    connection = DummyConnection(row=None, execute_result="INSERT 0 0")
    item_id = new_id()
    moderation_result_id = new_id()

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(pe_repo, "get_pg_connection", conn_stub)

    repo = pe_repo.ProcessedEventRepository()
    first_time = await repo.register_processing(
        event_id=f"moderation:{item_id}:{moderation_result_id}",
        item_id=item_id,
        moderation_result_id=moderation_result_id,
    )

    assert first_time is False
    assert len(connection.executed) == 1


@pytest.mark.asyncio
async def test_advertisement_repository_close_success(monkeypatch):
    item_id = new_id()
    expected = {
        "item_id": item_id,
        "moderation_result_ids": [new_id(), new_id()],
    }
    connection = DummyConnection(expected)

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    result = await repo.close(item_id=item_id)

    assert result is not None
    assert result.item_id == item_id
    assert result.moderation_result_ids == expected["moderation_result_ids"]
    assert len(connection.fetched) == 1
    assert "DELETE FROM moderation_results" in connection.fetched[0][0]
    assert "DELETE FROM advertisements" in connection.fetched[0][0]


@pytest.mark.asyncio
async def test_advertisement_repository_close_not_found(monkeypatch):
    connection = DummyConnection(None)
    missing_item_id = new_id()

    @asynccontextmanager
    async def conn_stub():
        yield connection

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    result = await repo.close(item_id=missing_item_id)

    assert result is None
    assert len(connection.fetched) == 1


@pytest.mark.asyncio
async def test_advertisement_repository_close_raises_storage_unavailable(monkeypatch):
    item_id = new_id()

    @asynccontextmanager
    async def conn_stub():
        raise RuntimeError("db unavailable")
        yield

    monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

    repo = ads_repo.AdvertisementRepository()
    with pytest.raises(StorageUnavailableError):
        await repo.close(item_id=item_id)
