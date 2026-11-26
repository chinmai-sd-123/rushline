"""Liveness and readiness probes."""

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

CHECK_TIMEOUT_SECONDS = 1.0

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness: the process is up. Never touches dependencies."""
    return {"status": "ok"}


async def postgres_ok(engine: AsyncEngine) -> bool:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SECONDS):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        return True
    except Exception:  # any failure means not ready
        return False


async def redis_ok(redis: Redis) -> bool:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SECONDS):
            await redis.ping()
        return True
    except Exception:
        return False


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    """Readiness: Postgres is required, Redis only degrades (seat maps fall back to Postgres)."""
    pg, rd = await asyncio.gather(
        postgres_ok(request.app.state.engine), redis_ok(request.app.state.redis)
    )
    if not pg:
        status, code = "unavailable", 503
    elif not rd:
        status, code = "degraded", 200
    else:
        status, code = "ok", 200
    checks = {"postgres": "ok" if pg else "down", "redis": "ok" if rd else "down"}
    return JSONResponse({"status": status, "checks": checks}, status_code=code)
