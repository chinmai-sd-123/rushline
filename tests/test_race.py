"""The v0 milestone: naive holds double book under concurrency (ADR 002 baseline).

Whether a race happens by itself depends on timing (it did on the dev machine, not on
CI). The test removes the luck: every racer is paused between its read and its write,
which is the worst interleaving the naive strategy allows.
"""

import asyncio
import os
import uuid

import httpx2
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from rushline import holds
from rushline.main import app

RACERS = 10  # below the pool size (15): paused racers each hold a connection
# Different users, so the one-open-hold index never decides the race.
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
                    client.post(
                        "/holds",
                        json={"show_id": 1, "seat_ids": SEATS},
                        headers={**auth(u), "Idempotency-Key": uuid.uuid4().hex},
                    )
                    for u in user_ids
                )
            )


async def add_racers() -> list[int]:
    users = await query(
        "INSERT INTO users (email, name) "
        "SELECT 'racer' || g || '@example.com', 'Racer ' || g FROM generate_series(1, :n) g "
        "RETURNING id",
        n=RACERS,
    )
    return [u.id for u in users]


def once_per_request(barrier: asyncio.Barrier):
    """Pause each request once; retries (serializable) must not wait again."""
    seen: set[asyncio.Task] = set()

    async def wait() -> None:
        task = asyncio.current_task()
        if task not in seen:
            seen.add(task)
            await barrier.wait()

    return wait


@pytest.mark.anyio
async def test_naive_holds_double_book_under_concurrency(db, auth, monkeypatch):
    monkeypatch.setenv("RUSHLINE_HOLD_STRATEGY", "naive")
    monkeypatch.setattr(holds, "after_naive_read", asyncio.Barrier(RACERS).wait)

    responses = await race(auth, await add_racers())

    winners = [r for r in responses if r.status_code == 201]
    # The bug: every racer saw "available", so every one was told they hold the seats...
    assert len(winners) == RACERS
    # ...but each seat row can point at only one hold, so the rest hold nothing.
    [(owners,)] = await query(
        "SELECT count(DISTINCT hold_id) FROM seats WHERE id = ANY(:ids)", ids=SEATS
    )
    [(active,)] = await query("SELECT count(*) FROM holds WHERE status = 'active'")
    assert active == len(winners) > owners


SAFE = ["pessimistic", "pessimistic_nowait", "optimistic", "atomic", "serializable"]


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", SAFE)
async def test_safe_strategies_give_the_seats_to_exactly_one_racer(db, auth, monkeypatch, strategy):
    monkeypatch.setenv("RUSHLINE_HOLD_STRATEGY", strategy)
    # Everyone reaches the claim at the same moment: maximum contention.
    monkeypatch.setattr(holds, "before_claim", once_per_request(asyncio.Barrier(RACERS)))

    responses = await race(auth, await add_racers())

    assert sorted(r.status_code for r in responses) == [201] + [409] * (RACERS - 1)
    [(owners,)] = await query(
        "SELECT count(DISTINCT hold_id) FROM seats WHERE id = ANY(:ids)", ids=SEATS
    )
    [(active,)] = await query("SELECT count(*) FROM holds WHERE status = 'active'")
    assert owners == active == 1  # losers' holds were rolled back


@pytest.mark.anyio
async def test_one_user_racing_for_different_seats_gets_one_hold(db, auth, monkeypatch):
    """The partial unique index decides: only one open hold per user per show."""
    monkeypatch.setenv("RUSHLINE_HOLD_STRATEGY", "atomic")
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://test") as client:
            responses = await asyncio.gather(
                *(
                    client.post(
                        "/holds",
                        json={"show_id": 1, "seat_ids": [seat]},
                        headers={**auth(1), "Idempotency-Key": uuid.uuid4().hex},
                    )
                    for seat in range(1, RACERS + 1)
                )
            )

    assert sorted(r.status_code for r in responses) == [201] + [409] * (RACERS - 1)
    [(active,)] = await query("SELECT count(*) FROM holds WHERE user_id = 1")
    assert active == 1
