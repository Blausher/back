from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import os
from typing import Any

import jwt

from app.errors import AccountBlockedError, InvalidCredentialsError, InvalidTokenError
from app.models.access_token import AccessTokenPayload
from app.models.account import Account
from app.repositories.accounts import AccountRepository


def _decode_numeric_date(value: Any) -> datetime:
    """Нормализует JWT date claim в timezone-aware datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    raise InvalidTokenError("JWT token is invalid")


@dataclass
class AuthService:
    """Сервис авторизации и работы с access JWT."""

    account_repository: AccountRepository = field(default_factory=AccountRepository)
    jwt_secret: str = field(default_factory=lambda: os.getenv("AUTH_JWT_SECRET", "dev-secret"))
    jwt_ttl_seconds: int = field(default_factory=lambda: int(os.getenv("AUTH_JWT_TTL_SECONDS", "3600")))
    jwt_algorithm: str = "HS256"

    async def authorize(self, login: str, password: str) -> str:
        """Проверяет credentials и выдает access token."""
        account = await self.account_repository.get_by_login_and_password(login, password)
        if account is None:
            raise InvalidCredentialsError("Invalid credentials")
        if account.is_blocked:
            raise AccountBlockedError("Account is blocked")
        return self.issue_token(account)

    def issue_token(self, account: Account) -> str:
        """Создает подписанный access JWT для аккаунта."""
        issued_at = datetime.now(timezone.utc)
        expires_at = issued_at + timedelta(seconds=self.jwt_ttl_seconds)
        payload = {
            "sub": str(account.id),
            "login": account.login,
            "iat": issued_at,
            "exp": expires_at,
            "type": "access",
        }
        return jwt.encode(payload, self.jwt_secret, algorithm=self.jwt_algorithm)

    def verify_token(self, token: str) -> AccessTokenPayload:
        """Проверяет access JWT и возвращает его payload."""
        try:
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=[self.jwt_algorithm],
                options={"require": ["sub", "login", "iat", "exp", "type"]},
            )
        except jwt.PyJWTError as exc:
            raise InvalidTokenError("JWT token is invalid") from exc

        if payload.get("type") != "access":
            raise InvalidTokenError("JWT token is invalid")

        try:
            account_id = int(payload["sub"])
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidTokenError("JWT token is invalid") from exc
        if account_id < 1:
            raise InvalidTokenError("JWT token is invalid")

        login = payload.get("login")
        if not isinstance(login, str) or not login:
            raise InvalidTokenError("JWT token is invalid")

        issued_at = _decode_numeric_date(payload["iat"])
        expires_at = _decode_numeric_date(payload["exp"])

        return AccessTokenPayload(
            account_id=account_id,
            login=login,
            issued_at=issued_at,
            expires_at=expires_at,
        )
