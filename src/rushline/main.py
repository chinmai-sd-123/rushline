import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from rushline.config import Settings
from rushline.db import create_engine
from rushline.shows import router as shows_router

CHECK_TIMEOUT_SECONDS = 1.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    app.state.engine = create_engine(settings)
    app.state.redis = Redis.from_url(settings.redis_url)
    yield
    await app.state.redis.aclose()
    await app.state.engine.dispose()


app = FastAPI(title="Rushline", lifespan=lifespan)
app.include_router(shows_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def postgres_ok(engine: AsyncEngine) -> bool:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SECONDS):
            async with engine.connect() as conn:
                await conn.execute(text("select 1"))
        return True
    except Exception:
        return False


async def redis_ok(redis: Redis) -> bool:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SECONDS):
            await redis.ping()
        return True
    except Exception:
        return False


@app.get("/ready")
async def ready(request: Request) -> JSONResponse:
    pg, rd = await asyncio.gather(
        postgres_ok(request.app.state.engine),
        redis_ok(request.app.state.redis),
    )
    if not pg:
        status, code = "unavailable", 503
    elif not rd:
        status, code = "degraded", 200
    else:
        status, code = "ok", 200
    checks = {"postgres": "ok" if pg else "down", "redis": "ok" if rd else "down"}
    return JSONResponse({"status": status, "checks": checks}, status_code=code)
