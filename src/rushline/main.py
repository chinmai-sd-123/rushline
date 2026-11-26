"""App wiring: shared clients are created once at startup and closed at shutdown."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

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


app = FastAPI(title="Rushline", lifespan=lifespan)
app.add_middleware(RequestContextMiddleware)
for module in (health, auth, shows, holds, bookings):
    app.include_router(module.router)
