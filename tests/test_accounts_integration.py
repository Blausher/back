import os
import time

import asyncpg
import pytest

from app.errors import AccountAlreadyExistsError
from app.repositories.accounts import AccountRepository


def _configure_default_pg_env() -> None:
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "15432")
    os.environ.setdefault("POSTGRES_USER", "blausher")
    os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
    os.environ.setdefault("POSTGRES_DB", "back")


def _pg_dsn() -> str:
    return (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )


async def _require_live_postgres() -> None:
    _configure_default_pg_env()
    try:
        connection = await asyncpg.connect(_pg_dsn(), timeout=1)
    except Exception as exc:  # pragma: no cover - depends on runtime env
        pytest.skip(f"PostgreSQL is unavailable for integration test: {exc}")
    try:
        await connection.execute("SELECT 1")
        await connection.execute("SELECT 1 FROM account LIMIT 1")
    except asyncpg.UndefinedTableError as exc:  # pragma: no cover - depends on runtime env
        pytest.skip(f"PostgreSQL schema is outdated for integration test: {exc}")
    finally:
        await connection.close()


async def _cleanup_account(login: str) -> None:
    connection = await asyncpg.connect(_pg_dsn(), timeout=1)
    try:
        await connection.execute("DELETE FROM account WHERE login = $1", login)
    finally:
        await connection.close()


def _new_login() -> str:
    return f"account_test_{time.time_ns()}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_create_returns_created_account():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()
    password = "s3cret-password"

    await _cleanup_account(login)
    try:
        created = await repo.create(login=login, password=password)

        assert created.login == login
        assert created.password != password
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_get_by_id_returns_created_account():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()
    password = "s3cret-password"

    await _cleanup_account(login)
    try:
        created = await repo.create(login=login, password=password)
        loaded = await repo.get_by_id(created.id)

        assert loaded is not None
        assert loaded.id == created.id
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_get_by_login_and_password_returns_account_for_valid_password():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()
    password = "s3cret-password"

    await _cleanup_account(login)
    try:
        created = await repo.create(login=login, password=password)
        found = await repo.get_by_login_and_password(login=login, password=password)

        assert found is not None
        assert found.id == created.id
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_get_by_login_and_password_returns_none_for_invalid_password():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()
    password = "s3cret-password"

    await _cleanup_account(login)
    try:
        await repo.create(login=login, password=password)
        missing = await repo.get_by_login_and_password(login=login, password="wrong-password")

        assert missing is None
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_block_marks_account_as_blocked():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()

    await _cleanup_account(login)
    try:
        created = await repo.create(login=login, password="password")
        blocked = await repo.block(created.id)

        assert blocked is not None
        assert blocked.is_blocked is True
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_delete_removes_existing_account():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()

    await _cleanup_account(login)
    try:
        created = await repo.create(login=login, password="password")
        deleted = await repo.delete(created.id)
        loaded = await repo.get_by_id(created.id)

        assert deleted is True
        assert loaded is None
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_delete_returns_false_for_missing_account():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()

    await _cleanup_account(login)
    try:
        created = await repo.create(login=login, password="password")
        await repo.delete(created.id)
        deleted_missing = await repo.delete(created.id)

        assert deleted_missing is False
    finally:
        await _cleanup_account(login)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_account_repository_create_raises_for_duplicate_login():
    await _require_live_postgres()
    repo = AccountRepository()
    login = _new_login()

    await _cleanup_account(login)
    try:
        await repo.create(login=login, password="password")

        with pytest.raises(AccountAlreadyExistsError):
            await repo.create(login=login, password="another-password")
    finally:
        await _cleanup_account(login)
