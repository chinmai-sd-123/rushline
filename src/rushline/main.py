"""App wiring: shared clients are created once at startup and closed at shutdown."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from redis.asyncio import BlockingConnectionPool, Redis
from sqlalchemy.exc import TimeoutError as PoolTimeout

from rushline import auth, bookings, health, holds, policies, runs, shows
from rushline.cache import SeatMapCache
from rushline.config import Settings
from rushline.db import create_engine
from rushline.embeddings import make_embedder
from rushline.observability import RequestContextMiddleware, configure_logging
from rushline.payments import PaymentClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    configure_logging(settings.log_level)
    app.state.settings = settings
    app.state.engine = create_engine(settings)  # lazy: nothing connects until first use
    # Bounded pool and short timeouts: a burst waits briefly for a connection instead of
    # opening hundreds, and a hung Redis can't hang requests (cache and limits fall back).
    app.state.redis = Redis(
        connection_pool=BlockingConnectionPool.from_url(
            settings.redis_url,
            max_connections=50,
            timeout=0.5,
            socket_timeout=0.5,
            socket_connect_timeout=0.5,
        )
    )
    # Live run streams block on Redis for up to 15s: their own client, never the shared pool.
    app.state.redis_streams = Redis.from_url(settings.redis_url, socket_timeout=30)
    app.state.seat_maps = SeatMapCache(
        app.state.redis, settings.seat_map_fresh_seconds, settings.seat_map_stale_seconds
    )
    app.state.payments = PaymentClient(settings.payment_url, settings.payment_timeout_seconds)
    app.state.embedder = make_embedder(settings)
    yield
    await app.state.seat_maps.aclose()
    await app.state.payments.aclose()
    await app.state.redis_streams.aclose()
    await app.state.redis.aclose()
    await app.state.engine.dispose()


log = logging.getLogger("rushline")

app = FastAPI(title="Rushline", lifespan=lifespan)
app.add_middleware(RequestContextMiddleware)


@app.exception_handler(PoolTimeout)
async def pool_exhausted(request: Request, exc: PoolTimeout) -> JSONResponse:
    """Every pooled connection is busy: shed load with a retryable 503, not a 500."""
    log.warning("db pool exhausted", extra={"fields": {"path": request.url.path}})
    return JSONResponse(
        {"detail": "server busy, try again"}, status_code=503, headers={"Retry-After": "1"}
    )


@app.exception_handler(OSError)
async def database_down(request: Request, exc: OSError) -> JSONResponse:
    """Postgres is unreachable (design: Risks).

    OSError, not just ConnectionRefusedError: when localhost resolves to both IPv4 and
    IPv6, asyncio raises a plain OSError wrapping both refusals. Redis and payment
    network errors are caught where they happen, so only database errors get here.
    """
    log.error("database unreachable", extra={"fields": {"path": request.url.path}})
    return JSONResponse(
        {"detail": "database unavailable"}, status_code=503, headers={"Retry-After": "5"}
    )


for module in (health, auth, shows, holds, bookings, runs, policies):
    app.include_router(module.router)

CHAT_PAGE = Path(__file__).parent / "static" / "chat.html"


@app.get("/chat", include_in_schema=False)
async def chat_page() -> FileResponse:
    """The one simple chat page (design: no polished frontend)."""
    return FileResponse(CHAT_PAGE)
