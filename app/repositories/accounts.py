from __future__ import annotations

from dataclasses import dataclass
import hashlib
from time import perf_counter
from typing import Any, Mapping

from asyncpg import exceptions as pg_exc

from app.clients.postgres import get_pg_connection
from app.errors import AccountAlreadyExistsError, StorageUnavailableError
from app.models.account import Account
from app.observability.metrics import DB_QUERY_DURATION


def _hash_password(password: str) -> str:
    """Возвращает SHA-256 хеш пароля."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


async def _timed_fetchrow(connection, query_type: str, query: str, *args):
    """Выполняет fetchrow и пишет длительность запроса в метрики."""
    started_at = perf_counter()
    try:
        return await connection.fetchrow(query, *args)
    finally:
        DB_QUERY_DURATION.labels(query_type=query_type).observe(perf_counter() - started_at)


@dataclass(frozen=True)
class AccountStorage:
    async def create(self, login: str, password: str) -> Mapping[str, Any]:
        """Создает аккаунт и сохраняет хеш пароля."""
        query = """
            INSERT INTO account (login, password)
            VALUES ($1, $2)
            RETURNING id, login, password, is_blocked
        """
        hashed_password = _hash_password(password)
        try:
            async with get_pg_connection() as connection:
                record = await _timed_fetchrow(connection, "insert", query, login, hashed_password) # хешируем пароль перед записью в бд
        except pg_exc.UniqueViolationError as exc:
            raise AccountAlreadyExistsError("Account already exists") from exc
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        return dict(record)

    async def get_by_id(self, account_id: int) -> Mapping[str, Any] | None:
        """Возвращает аккаунт по id или None."""
        query = """
            SELECT id, login, password, is_blocked
            FROM account
            WHERE id = $1
        """
        try:
            async with get_pg_connection() as connection:
                record = await _timed_fetchrow(connection, "select", query, account_id)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        if record is None:
            return None
        return dict(record)

    async def delete(self, account_id: int) -> bool:
        """Удаляет аккаунт по id и возвращает факт удаления."""
        query = """
            DELETE FROM account
            WHERE id = $1
            RETURNING id
        """
        try:
            async with get_pg_connection() as connection:
                record = await _timed_fetchrow(connection, "delete", query, account_id)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        return record is not None

    async def block(self, account_id: int) -> Mapping[str, Any] | None:
        """Блокирует аккаунт и возвращает обновленную запись."""
        query = """
            UPDATE account
            SET is_blocked = TRUE
            WHERE id = $1
            RETURNING id, login, password, is_blocked
        """
        try:
            async with get_pg_connection() as connection:
                record = await _timed_fetchrow(connection, "update", query, account_id)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        if record is None:
            return None
        return dict(record)

    async def get_by_login_and_password(self, login: str, password: str) -> Mapping[str, Any] | None:
        """Ищет аккаунт по логину и исходному паролю."""
        query = """
            SELECT id, login, password, is_blocked
            FROM account
            WHERE login = $1
              AND password = $2
        """
        hashed_password = _hash_password(password)
        try:
            async with get_pg_connection() as connection:
                record = await _timed_fetchrow(connection, "select", query, login, hashed_password)
        except Exception as exc:
            raise StorageUnavailableError("Storage operation failed") from exc
        if record is None:
            return None
        return dict(record)


@dataclass(frozen=True)
class AccountRepository:
    account_storage: AccountStorage = AccountStorage()

    async def create(self, login: str, password: str) -> Account:
        """Создает аккаунт через storage и возвращает модель."""
        raw_account = await self.account_storage.create(login=login, password=password)
        return Account.model_validate(raw_account)

    async def get_by_id(self, account_id: int) -> Account | None:
        """Возвращает аккаунт по id или None."""
        raw_account = await self.account_storage.get_by_id(account_id=account_id)
        if raw_account is None:
            return None
        return Account.model_validate(raw_account)

    async def delete(self, account_id: int) -> bool:
        """Удаляет аккаунт и возвращает результат операции."""
        return await self.account_storage.delete(account_id=account_id)

    async def block(self, account_id: int) -> Account | None:
        """Блокирует аккаунт по id и возвращает модель."""
        raw_account = await self.account_storage.block(account_id=account_id)
        if raw_account is None:
            return None
        return Account.model_validate(raw_account)

    async def get_by_login_and_password(self, login: str, password: str) -> Account | None:
        """Возвращает аккаунт по логину и паролю или None."""
        raw_account = await self.account_storage.get_by_login_and_password(
            login=login,
            password=password,
        )
        if raw_account is None:
            return None
        return Account.model_validate(raw_account)
