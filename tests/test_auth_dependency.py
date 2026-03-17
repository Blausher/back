from starlette.requests import Request
import pytest
from fastapi import HTTPException

from app.dependencies import require_account
from app.errors import AccountBlockedError, InvalidTokenError
from app.models.account import Account
from tests.id_factory import new_id


def _build_request(cookie_header: str | None = None) -> Request:
    headers = []
    if cookie_header is not None:
        headers.append((b"cookie", cookie_header.encode("utf-8")))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/predict",
        "headers": headers,
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_require_account_returns_account():
    expected_account = Account(id=new_id(), login="tester", password="hashed", is_blocked=False)

    class DummyAuthService:
        async def verify(self, token: str) -> Account:
            assert token == "valid-token"
            return expected_account

    account = await require_account(
        _build_request("x-user-token=valid-token"),
        DummyAuthService(),
    )

    assert account == expected_account


@pytest.mark.asyncio
async def test_require_account_returns_401_when_cookie_missing():
    class DummyAuthService:
        async def verify(self, token: str) -> Account:
            raise AssertionError("verify should not be called without cookie")

    with pytest.raises(HTTPException) as exc_info:
        await require_account(_build_request(), DummyAuthService())

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Authentication required"


@pytest.mark.asyncio
async def test_require_account_returns_401_for_invalid_token():
    class DummyAuthService:
        async def verify(self, token: str) -> Account:
            raise InvalidTokenError("JWT token is invalid")

    with pytest.raises(HTTPException) as exc_info:
        await require_account(
            _build_request("x-user-token=broken-token"),
            DummyAuthService(),
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid authentication token"


@pytest.mark.asyncio
async def test_require_account_returns_403_for_blocked_account():
    class DummyAuthService:
        async def verify(self, token: str) -> Account:
            raise AccountBlockedError("Account is blocked")

    with pytest.raises(HTTPException) as exc_info:
        await require_account(
            _build_request("x-user-token=blocked-token"),
            DummyAuthService(),
        )

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "Account is blocked"
