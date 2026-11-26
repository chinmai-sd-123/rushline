"""The v0 milestone: naive holds double book under concurrency (ADR 002 baseline)."""

import asyncio
import os

import httpx2
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from rushline.main import app

RACERS = 20  # different users, so the one-open-hold index never decides the race
SEATS = [1, 2]


async def query(sql: str, **params):
    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.begin() as conn:
        rows = (await conn.execute(text(sql), params)).all()
    await engine.dispose()
    return rows


async def race(auth, user_ids: list[int]) -> list[httpx2.Response]:
    """Fire every hold request at once through the real app and its pool."""
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://test") as client:
            return await asyncio.gather(
                *(
                    client.post("/holds", json={"show_id": 1, "seat_ids": SEATS}, headers=auth(u))
                    for u in user_ids
                )
            )


@pytest.mark.anyio
async def test_naive_holds_double_book_under_concurrency(db, auth):
    users = await query(
        "INSERT INTO users (email, name) "
        "SELECT 'racer' || g || '@example.com', 'Racer ' || g FROM generate_series(1, :n) g "
        "RETURNING id",
        n=RACERS,
    )

    responses = await race(auth, [u.id for u in users])

    winners = [r for r in responses if r.status_code == 201]
    assert {r.status_code for r in responses} <= {201, 409}
    # The bug: more than one user was told they hold the same seats...
    assert len(winners) > 1
    # ...but each seat row can point at only one hold, so the rest hold nothing.
    [(owners,)] = await query(
        "SELECT count(DISTINCT hold_id) FROM seats WHERE id = ANY(:ids)", ids=SEATS
    )
    [(active,)] = await query("SELECT count(*) FROM holds WHERE status = 'active'")
    assert active == len(winners) > owners
