from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.errors import AccountBlockedError, InvalidCredentialsError
from app.models.login import LoginRequest
from app.services.auth import AuthService

router = APIRouter()
auth_service = AuthService()


@router.get("/")
async def root():
    return {"message": "Hello World"}


@router.post("/login")
async def login(dto: LoginRequest) -> Response:
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
