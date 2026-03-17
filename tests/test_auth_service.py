from datetime import datetime, timedelta, timezone

import jwt
import pytest

from app.errors import AccountBlockedError, InvalidCredentialsError, InvalidTokenError
from app.models.account import Account
from app.services.auth import AuthService
from tests.id_factory import new_id


TEST_SECRET = "test-secret-with-32-characters!!"
OTHER_SECRET = "other-secret-with-32-characters!"


class FakeAccountRepository:
    def __init__(self, account: Account | None):
        self.account = account
        self.calls: list[tuple[str, str]] = []

    async def get_by_login_and_password(self, login: str, password: str) -> Account | None:
        self.calls.append((login, password))
        return self.account


@pytest.mark.asyncio
async def test_authorize_returns_access_token(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    monkeypatch.setenv("AUTH_JWT_TTL_SECONDS", "3600")
    account_id = new_id()
    account = Account(id=account_id, login="tester", password="hashed", is_blocked=False)
    repo = FakeAccountRepository(account)
    service = AuthService(account_repository=repo)

    token = await service.authorize("tester", "password")
    payload = service.verify_token(token)

    assert isinstance(token, str)
    assert repo.calls == [("tester", "password")]
    assert payload.account_id == account_id
    assert payload.login == "tester"


@pytest.mark.asyncio
async def test_authorize_raises_for_invalid_credentials(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    repo = FakeAccountRepository(None)
    service = AuthService(account_repository=repo)

    with pytest.raises(InvalidCredentialsError):
        await service.authorize("tester", "wrong-password")


@pytest.mark.asyncio
async def test_authorize_raises_for_blocked_account(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    blocked_account = Account(id=new_id(), login="tester", password="hashed", is_blocked=True)
    repo = FakeAccountRepository(blocked_account)
    service = AuthService(account_repository=repo)

    with pytest.raises(AccountBlockedError):
        await service.authorize("tester", "password")


def test_verify_token_returns_payload(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    monkeypatch.setenv("AUTH_JWT_TTL_SECONDS", "120")
    service = AuthService(account_repository=FakeAccountRepository(None))
    account_id = new_id()
    account = Account(id=account_id, login="reader", password="hashed", is_blocked=False)

    token = service.issue_token(account)
    payload = service.verify_token(token)

    assert payload.account_id == account_id
    assert payload.login == "reader"
    assert payload.expires_at > payload.issued_at


def test_verify_token_rejects_expired_token(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    service = AuthService(account_repository=FakeAccountRepository(None))
    now = datetime.now(timezone.utc)
    account_id = new_id()
    token = jwt.encode(
        {
            "sub": str(account_id),
            "login": "reader",
            "iat": now - timedelta(seconds=10),
            "exp": now - timedelta(seconds=1),
            "type": "access",
        },
        TEST_SECRET,
        algorithm="HS256",
    )

    with pytest.raises(InvalidTokenError):
        service.verify_token(token)


def test_verify_token_rejects_invalid_signature(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    service = AuthService(account_repository=FakeAccountRepository(None))
    now = datetime.now(timezone.utc)
    account_id = new_id()
    token = jwt.encode(
        {
            "sub": str(account_id),
            "login": "reader",
            "iat": now,
            "exp": now + timedelta(seconds=60),
            "type": "access",
        },
        OTHER_SECRET,
        algorithm="HS256",
    )

    with pytest.raises(InvalidTokenError):
        service.verify_token(token)


def test_verify_token_rejects_missing_sub(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    service = AuthService(account_repository=FakeAccountRepository(None))
    now = datetime.now(timezone.utc)
    token = jwt.encode(
        {
            "login": "reader",
            "iat": now,
            "exp": now + timedelta(seconds=60),
            "type": "access",
        },
        TEST_SECRET,
        algorithm="HS256",
    )

    with pytest.raises(InvalidTokenError):
        service.verify_token(token)


def test_verify_token_rejects_wrong_type(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", TEST_SECRET)
    service = AuthService(account_repository=FakeAccountRepository(None))
    now = datetime.now(timezone.utc)
    account_id = new_id()
    token = jwt.encode(
        {
            "sub": str(account_id),
            "login": "reader",
            "iat": now,
            "exp": now + timedelta(seconds=60),
            "type": "refresh",
        },
        TEST_SECRET,
        algorithm="HS256",
    )

    with pytest.raises(InvalidTokenError):
        service.verify_token(token)
