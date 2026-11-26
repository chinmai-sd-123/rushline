import asyncio

import httpx2
import pytest
from helpers import aquery
from redis.asyncio import Redis

from rushline.cache import SeatMapCache
from rushline.main import app


@pytest.fixture
async def redis(anyio_backend):
    from rushline.config import Settings

    r = Redis.from_url(Settings().redis_url)
    yield r
    await r.aclose()


def slow_loader(calls: list, delay: float = 0.1):
    async def load() -> dict:
        calls.append(1)
        await asyncio.sleep(delay)  # a slow query: everyone else piles up behind it
        return {"show_id": 1, "seats": []}

    return load


async def app_client():
    transport = httpx2.ASGITransport(app=app)
    return httpx2.AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.anyio
async def test_first_read_misses_then_hits(db):
    async with app.router.lifespan_context(app):
        async with await app_client() as client:
            first = await client.get("/shows/1/seats")
            second = await client.get("/shows/1/seats")
            loads = app.state.seat_maps.loads

    assert first.headers["x-cache"] == "miss"
    assert second.headers["x-cache"] == "hit"
    assert second.json() == first.json()
    assert loads == 1


@pytest.mark.anyio
async def test_cold_cache_stampede_costs_one_database_read(db):
    async with app.router.lifespan_context(app):
        async with await app_client() as client:
            responses = await asyncio.gather(*(client.get("/shows/1/seats") for _ in range(200)))
            loads = app.state.seat_maps.loads

    assert {r.status_code for r in responses} == {200}
    assert loads == 1


@pytest.mark.anyio
async def test_two_processes_share_one_load_through_the_redis_lock(db, redis):
    calls: list = []
    a, b = SeatMapCache(redis), SeatMapCache(redis)  # two API processes

    results = await asyncio.gather(a.get(1, slow_loader(calls)), b.get(1, slow_loader(calls)))

    assert len(calls) == 1
    assert sorted(status for _, status in results) == ["miss", "miss"]


@pytest.mark.anyio
async def test_stale_entry_is_served_while_one_refresh_runs(db, redis):
    calls: list = []
    cache = SeatMapCache(redis, fresh_seconds=0)  # everything is stale at once
    await cache.get(1, slow_loader(calls, 0))

    statuses = [
        s for _, s in await asyncio.gather(*(cache.get(1, slow_loader(calls)) for _ in range(20)))
    ]
    await cache.aclose()  # wait for the background refresh

    assert set(statuses) == {"stale"}  # nobody waited for the database
    assert len(calls) == 2  # the first load, plus exactly one refresh


@pytest.mark.anyio
async def test_redis_down_falls_back_to_postgres(db, monkeypatch):
    monkeypatch.setenv("RUSHLINE_REDIS_URL", "redis://localhost:1/0")
    async with app.router.lifespan_context(app):
        async with await app_client() as client:
            response = await client.get("/shows/1/seats")

    assert response.status_code == 200
    assert response.headers["x-cache"] == "bypass"
    assert len(response.json()["seats"]) == 50


@pytest.mark.anyio
async def test_unknown_show_is_404_and_not_cached(db, redis):
    async with app.router.lifespan_context(app):
        async with await app_client() as client:
            response = await client.get("/shows/999/seats")

    assert response.status_code == 404
    assert await redis.get("seatmap:999") is None
    assert (await aquery("SELECT 1"))[0][0] == 1


@pytest.mark.anyio
async def test_stampede_with_redis_down_still_costs_one_database_read(db, monkeypatch):
    monkeypatch.setenv("RUSHLINE_REDIS_URL", "redis://localhost:1/0")
    async with app.router.lifespan_context(app):
        async with await app_client() as client:
            responses = await asyncio.gather(*(client.get("/shows/1/seats") for _ in range(200)))
            loads = app.state.seat_maps.loads

    assert {r.headers["x-cache"] for r in responses} == {"bypass"}
    assert loads == 1
