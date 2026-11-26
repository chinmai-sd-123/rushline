"""Small shared helpers for tests that talk to the database directly."""

import asyncio
import os
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


async def aquery(sql: str, **params) -> list:
    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.begin() as conn:
        result = await conn.execute(text(sql), params)
        rows = result.all() if result.returns_rows else []
    await engine.dispose()
    return rows


def query(sql: str, **params) -> list:
    return asyncio.run(aquery(sql, **params))


def hold(client, headers, seat_ids, show_id=1):
    response = client.post(
        "/holds",
        json={"show_id": show_id, "seat_ids": seat_ids},
        headers={**headers, "Idempotency-Key": uuid.uuid4().hex},
    )
    assert response.status_code == 201, response.text
    return response.json()["hold_id"]


def book(client, headers, hold_id, token="tok_ok", key=None):
    return client.post(
        "/bookings",
        json={"hold_id": hold_id, "payment_token": token},
        headers={**headers, "Idempotency-Key": key or uuid.uuid4().hex},
    )
