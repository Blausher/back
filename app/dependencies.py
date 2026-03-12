from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from app.errors import AccountBlockedError, InvalidTokenError
from app.models.account import Account
from app.services.auth import AuthService


def get_auth_service() -> AuthService:
    """Создает экземпляр auth-сервиса для FastAPI dependency injection."""
    return AuthService()


AuthServiceDepend = Annotated[AuthService, Depends(get_auth_service)]


async def require_account(request: Request, auth_service: AuthServiceDepend) -> Account:
    """Проверяет cookie с access token и возвращает авторизованный аккаунт."""
    user_token = request.cookies.get("x-user-token")
    if not user_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )

    try:
        return await auth_service.verify(user_token)
    except InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
        ) from exc
    except AccountBlockedError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is blocked",
        ) from exc
