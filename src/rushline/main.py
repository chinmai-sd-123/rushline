"""App wiring: shared clients are created once at startup and closed at shutdown."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy.exc import TimeoutError as PoolTimeout

from rushline import auth, bookings, health, holds, shows
from rushline.config import Settings
from rushline.db import create_engine
from rushline.observability import RequestContextMiddleware, configure_logging
from rushline.payments import PaymentClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    configure_logging(settings.log_level)
    app.state.settings = settings
    app.state.engine = create_engine(settings)  # lazy: nothing connects until first use
    # Short timeouts: a hung Redis must not hang requests (rate limits fail open).
    app.state.redis = Redis.from_url(
        settings.redis_url, socket_timeout=0.5, socket_connect_timeout=0.5
    )
    app.state.payments = PaymentClient(settings.payment_url, settings.payment_timeout_seconds)
    yield
    await app.state.payments.aclose()
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


for module in (health, auth, shows, holds, bookings):
    app.include_router(module.router)
