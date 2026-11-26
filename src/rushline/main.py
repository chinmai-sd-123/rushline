"""App wiring: shared clients are created once at startup and closed at shutdown."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from rushline import auth, health, holds, shows
from rushline.config import Settings
from rushline.db import create_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    app.state.settings = settings
    app.state.engine = create_engine(settings)  # lazy: nothing connects until first use
    app.state.redis = Redis.from_url(settings.redis_url)
    yield
    await app.state.redis.aclose()
    await app.state.engine.dispose()


app = FastAPI(title="Rushline", lifespan=lifespan)
for module in (health, auth, shows, holds):
    app.include_router(module.router)
