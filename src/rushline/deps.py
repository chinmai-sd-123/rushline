from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncConnection


async def get_conn(request: Request) -> AsyncIterator[AsyncConnection]:
    async with request.app.state.engine.connect() as conn:
        yield conn
