"""JWT auth. Every token names the user and who is acting for them (ADR 004)."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal, get_args

import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.config import Settings
from rushline.deps import get_conn, get_settings

Actor = Literal["user", "agent", "mcp"]
ALGORITHM = "HS256"

router = APIRouter(prefix="/auth")
_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    user_id: int
    actor: Actor
    run_id: int | None = None  # set only for agent tokens


def create_token(
    settings: Settings,
    user_id: int,
    actor: Actor = "user",
    run_id: int | None = None,
    ttl_seconds: int | None = None,
) -> str:
    now = datetime.now(UTC)
    ttl = settings.jwt_ttl_seconds if ttl_seconds is None else ttl_seconds
    claims = {
        "sub": str(user_id),  # JWT spec: sub is a string
        "actor": actor,
        "iat": now,
        "exp": now + timedelta(seconds=ttl),
    }
    if run_id is not None:
        claims["run_id"] = run_id
    return jwt.encode(claims, settings.jwt_secret, algorithm=ALGORITHM)


def decode_token(settings: Settings, token: str) -> Principal:
    # Pin the algorithm: never trust the token's own "alg" header.
    claims = jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=[ALGORITHM],
        options={"require": ["sub", "exp", "actor"]},
    )
    actor, run_id = claims["actor"], claims.get("run_id")
    if actor not in get_args(Actor) or (actor == "agent") != (run_id is not None):
        raise jwt.InvalidTokenError("bad actor claims")
    return Principal(user_id=int(claims["sub"]), actor=actor, run_id=run_id)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def current_principal(
    settings: Annotated[Settings, Depends(get_settings)],
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> Principal:
    if creds is None:
        raise _unauthorized("missing token")
    try:
        return decode_token(settings, creds.credentials)
    except (jwt.InvalidTokenError, ValueError) as exc:
        raise _unauthorized("invalid token") from exc


def require_actor(*allowed: Actor):
    """Dependency factory: 403 unless the caller's actor is allowed on this route."""

    async def check(principal: Annotated[Principal, Depends(current_principal)]) -> Principal:
        if principal.actor not in allowed:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"{principal.actor} not allowed")
        return principal

    return check


class DevTokenIn(BaseModel):
    email: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class Me(BaseModel):
    user_id: int
    actor: Actor
    run_id: int | None


@router.post("/dev-token")
async def dev_token(
    body: DevTokenIn,
    settings: Annotated[Settings, Depends(get_settings)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> TokenOut:
    """Seeded-user login for local demos. Off unless RUSHLINE_ENABLE_DEV_LOGIN=true."""
    if not settings.enable_dev_login:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "not found")
    user_id = (
        await conn.execute(text("SELECT id FROM users WHERE email = :email"), {"email": body.email})
    ).scalar_one_or_none()
    if user_id is None:
        raise _unauthorized("unknown user")
    return TokenOut(access_token=create_token(settings, user_id))


@router.get("/me")
async def me(principal: Annotated[Principal, Depends(current_principal)]) -> Me:
    return Me(user_id=principal.user_id, actor=principal.actor, run_id=principal.run_id)
