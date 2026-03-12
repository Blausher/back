import logging

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.errors import (
    AccountAlreadyExistsError,
    AccountBlockedError,
    InvalidCredentialsError,
    StorageUnavailableError,
)
from app.models.account_create import AccountCreateRequest
from app.models.account_public import AccountPublic
from app.models.login import LoginRequest
from app.repositories.accounts import AccountRepository
from app.services.auth import AuthService

router = APIRouter()
logger = logging.getLogger(__name__)
auth_service = AuthService()
account_repository = AccountRepository()


@router.get("/")
async def root():
    return {"message": "Hello World"}


@router.post("/accounts", response_model=AccountPublic, status_code=status.HTTP_201_CREATED)
async def create_account(dto: AccountCreateRequest) -> AccountPublic:
    '''Создание аккаунта'''
    try:
        account = await account_repository.create(dto.login, dto.password)
    except AccountAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Account already exists",
        ) from exc
    except StorageUnavailableError as exc:
        logger.exception("Create account failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc

    return AccountPublic(
        id=account.id,
        login=account.login,
        is_blocked=account.is_blocked,
    )


@router.post("/login")
async def login(dto: LoginRequest) -> Response:
    '''Логинимся в аккаунт'''
    try:
        user_token = await auth_service.authorize(dto.login, dto.password)
    except InvalidCredentialsError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid login or password",
        ) from exc
    except AccountBlockedError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is blocked",
        ) from exc

    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="x-user-token",
        value=user_token,
        secure=True,
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
