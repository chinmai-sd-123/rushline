"""Shared FastAPI dependencies."""

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.config import Settings


async def get_conn(request: Request) -> AsyncIterator[AsyncConnection]:
    """One pooled connection per request, returned to the pool even on errors."""
    async with request.app.state.engine.connect() as conn:
        yield conn


def get_settings(request: Request) -> Settings:
    return request.app.state.settings
