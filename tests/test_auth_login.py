import pytest
import httpx

from app.errors import AccountBlockedError, InvalidCredentialsError
from app.main import app
from app.routers import root as root_router


@pytest.mark.asyncio
async def test_login_sets_user_token_cookie(monkeypatch):
    issued_tokens: list[tuple[str, str]] = []

    class DummyAuthService:
        async def authorize(self, login: str, password: str) -> str:
            issued_tokens.append((login, password))
            return "jwt-token-value"

    monkeypatch.setattr(root_router, "auth_service", DummyAuthService())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/login",
            json={"login": "tester", "password": "secret"},
        )

    assert response.status_code == 200
    assert response.json() == {"message": "Login successful"}
    assert issued_tokens == [("tester", "secret")]
    assert response.cookies.get("x-user-token") == "jwt-token-value"
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "Secure" in response.headers["set-cookie"]


@pytest.mark.asyncio
async def test_login_returns_401_for_invalid_credentials(monkeypatch):
    class DummyAuthService:
        async def authorize(self, login: str, password: str) -> str:
            raise InvalidCredentialsError("Invalid credentials")

    monkeypatch.setattr(root_router, "auth_service", DummyAuthService())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/login",
            json={"login": "tester", "password": "wrong"},
        )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid login or password"


@pytest.mark.asyncio
async def test_login_returns_403_for_blocked_account(monkeypatch):
    class DummyAuthService:
        async def authorize(self, login: str, password: str) -> str:
            raise AccountBlockedError("Account is blocked")

    monkeypatch.setattr(root_router, "auth_service", DummyAuthService())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/login",
            json={"login": "tester", "password": "secret"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "Account is blocked"


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
async def test_login_validates_payload(monkeypatch, payload):
    class DummyAuthService:
        async def authorize(self, login: str, password: str) -> str:
            raise AssertionError("authorize should not be called on invalid payload")

    monkeypatch.setattr(root_router, "auth_service", DummyAuthService())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post("/login", json=payload)

    assert response.status_code == 422
