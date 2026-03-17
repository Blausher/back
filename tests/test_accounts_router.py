import httpx
import pytest

from app.errors import AccountAlreadyExistsError, StorageUnavailableError
from app.main import app
from app.models.account import Account
from app.routers import root as root_router
from tests.id_factory import new_id


@pytest.mark.asyncio
async def test_create_account_returns_created_account(monkeypatch):
    account_id = new_id()

    class DummyAccountRepository:
        async def create(self, login: str, password: str) -> Account:
            assert login == "tester"
            assert password == "secret"
            return Account(id=account_id, login=login, password="hashed-secret", is_blocked=False)

    monkeypatch.setattr(root_router, "account_repository", DummyAccountRepository())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/accounts",
            json={"login": "tester", "password": "secret"},
        )

    assert response.status_code == 201
    assert response.json() == {
        "id": account_id,
        "login": "tester",
        "is_blocked": False,
    }


@pytest.mark.asyncio
async def test_create_account_returns_409_for_duplicate_login(monkeypatch):
    class DummyAccountRepository:
        async def create(self, login: str, password: str) -> Account:
            raise AccountAlreadyExistsError("Account already exists")

    monkeypatch.setattr(root_router, "account_repository", DummyAccountRepository())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/accounts",
            json={"login": "tester", "password": "secret"},
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "Account already exists"


@pytest.mark.asyncio
async def test_create_account_returns_500_when_storage_fails(monkeypatch):
    class DummyAccountRepository:
        async def create(self, login: str, password: str) -> Account:
            raise StorageUnavailableError("Storage operation failed")

    monkeypatch.setattr(root_router, "account_repository", DummyAccountRepository())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/accounts",
            json={"login": "tester", "password": "secret"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "Internal server error"


@pytest.mark.parametrize(
    "payload",
    [
        {"password": "secret"},
        {"login": "tester"},
        {"login": "", "password": "secret"},
        {"login": "tester", "password": ""},
    ],
)
@pytest.mark.asyncio
async def test_create_account_validates_payload(monkeypatch, payload):
    class DummyAccountRepository:
        async def create(self, login: str, password: str) -> Account:
            raise AssertionError("create should not be called on invalid payload")

    monkeypatch.setattr(root_router, "account_repository", DummyAccountRepository())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post("/accounts", json=payload)

    assert response.status_code == 422
