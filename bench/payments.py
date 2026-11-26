"""Payment reliability check (design: Rollout): inject provider timeouts, prove nothing is lost.

Each user holds one seat and pays. The mock provider times out on a share of ALL calls
after doing the work (the worst case: it happened, but nobody heard). The worker then
runs until everything settles, and our books are checked against the provider's:

- every captured payment has exactly one confirmed booking (no lost payments)
- every confirmed booking has a captured payment (no free seats)
- one payment per hold (no double charges), and no hold left in `paying`

    uv run python bench/payments.py --users 100 --timeout-rate 0.1
"""

import argparse
import asyncio
import os
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

ROOT = Path(__file__).resolve().parents[1]
BENCH_DB = "rushline_bench"


def configure_env(timeout_rate: float) -> str:
    sys.path.insert(0, str(ROOT / "src"))
    os.environ["MOCKPAY_TIMEOUT_RATE"] = str(timeout_rate)  # read when mockpay is imported
    from rushline.config import Settings

    base = make_url(Settings().database_url)
    os.environ |= {
        "RUSHLINE_DATABASE_URL": base.set(database=BENCH_DB).render_as_string(False),
        "RUSHLINE_PAYMENT_TIMEOUT_SECONDS": "0.2",
        "RUSHLINE_PAYMENT_WINDOW_SECONDS": "1",  # settle unknown payments quickly
        "RUSHLINE_CAPTURE_RETRY_AFTER_SECONDS": "0",
        "RUSHLINE_CAPTURE_MAX_ATTEMPTS": "20",
        "RUSHLINE_HOLDS_PER_MINUTE": "1000",
        "RUSHLINE_BOOKINGS_PER_MINUTE": "1000",
        "RUSHLINE_DB_POOL_SIZE": "40",
        "RUSHLINE_LOG_LEVEL": "ERROR",
    }
    return base.set(database="postgres").render_as_string(False)


async def create_database(admin_url: str) -> None:
    admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    async with admin.connect() as conn:
        if not await conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": BENCH_DB}
        ):
            await conn.execute(text(f"CREATE DATABASE {BENCH_DB}"))
    await admin.dispose()


async def scalar(sql: str):
    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.connect() as conn:
        value = await conn.scalar(text(sql))
    await engine.dispose()
    return value


async def rows(sql: str) -> list:
    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.connect() as conn:
        result = (await conn.execute(text(sql))).all()
    await engine.dispose()
    return result


async def setup(users: int) -> list[int]:
    from rushline.seed import load_demo_data

    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.begin() as conn:
        await load_demo_data(conn)  # 100 seats across 2 shows
        ids = await conn.scalars(
            text(
                "INSERT INTO users (email, name) "
                "SELECT 'pay' || g || '@example.com', 'Payer' FROM generate_series(1, :n) g "
                "RETURNING id"
            ),
            {"n": users},
        )
        user_ids = list(ids)
    await engine.dispose()
    return user_ids


async def run(users: int) -> None:
    import httpx2
    from redis.asyncio import Redis

    from rushline import mockpay, payments, worker
    from rushline.auth import create_token
    from rushline.config import Settings
    from rushline.db import create_engine
    from rushline.main import app
    from rushline.outbox import RedisStreamPublisher

    payments.transport_override = httpx2.ASGITransport(app=mockpay.app)
    settings = Settings()
    user_ids = await setup(users)
    seats = list(range(1, users + 1))  # one distinct seat per user: no seat contention here

    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx2.AsyncClient(transport=transport, base_url="http://bench") as client:

            async def hold_and_pay(user_id: int, seat: int) -> str:
                auth = {"Authorization": f"Bearer {create_token(settings, user_id)}"}
                show = 1 if seat <= 50 else 2
                held = await client.post(
                    "/holds",
                    json={"show_id": show, "seat_ids": [seat]},
                    headers={**auth, "Idempotency-Key": uuid.uuid4().hex},
                )
                if held.status_code != 201:
                    return f"hold {held.status_code}"
                paid = await client.post(
                    "/bookings",
                    json={"hold_id": held.json()["hold_id"], "payment_token": "tok_ok"},
                    headers={**auth, "Idempotency-Key": uuid.uuid4().hex},
                )
                return f"book {paid.status_code}"

            outcomes = Counter(
                await asyncio.gather(
                    *(hold_and_pay(u, s) for u, s in zip(user_ids, seats, strict=True))
                )
            )

    # Let the worker settle everything: unknown authorizes, failed captures.
    engine = create_engine(settings)
    redis = Redis.from_url(settings.redis_url)
    client = payments.PaymentClient(settings.payment_url, settings.payment_timeout_seconds)
    start = time.perf_counter()
    ticks, open_work = 0, None
    while open_work != 0 and ticks < 60:
        await asyncio.sleep(1.1)  # pass the 1s payment window
        await worker.tick(engine, client, RedisStreamPublisher(redis), settings)
        ticks += 1
        open_work = await scalar(
            "SELECT (SELECT count(*) FROM holds WHERE status = 'paying') "
            "+ (SELECT count(*) FROM bookings WHERE status = 'pending_capture')"
        )
    settle_seconds = time.perf_counter() - start
    await client.aclose()
    await redis.aclose()
    await engine.dispose()

    bookings = {
        r.payment_ref: r.status for r in await rows("SELECT payment_ref, status FROM bookings")
    }
    provider = list(mockpay.payments.values())
    captured = {p.id for p in provider if p.status == "captured"}
    authorized_left = [p.id for p in provider if p.status == "authorized"]
    confirmed = {ref for ref, s in bookings.items() if s == "confirmed"}
    holds = Counter(r.status for r in await rows("SELECT status FROM holds"))
    keys = Counter(p.key for p in provider)

    report = {
        "requests": dict(sorted(outcomes.items())),
        "faults injected": f"{mockpay.TIMEOUT_RATE:.0%} of every provider call timed out",
        "settled after": f"{ticks} worker ticks ({settle_seconds:.1f}s)",
        "holds": dict(holds),
        "bookings": dict(Counter(bookings.values())),
        "captured payments": len(captured),
        "lost payments (captured, no confirmed booking)": len(captured - confirmed),
        "free seats (confirmed, not captured)": len(confirmed - captured),
        "double charges (2+ payments for one hold)": sum(1 for n in keys.values() if n > 1),
        "holds left paying": holds.get("paying", 0),
        "authorizations left open": len(authorized_left),
    }
    for name, value in report.items():
        print(f"{name}: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--users", type=int, default=100, choices=range(1, 101), metavar="1-100")
    parser.add_argument("--timeout-rate", type=float, default=0.1)
    args = parser.parse_args()

    asyncio.run(create_database(configure_env(args.timeout_rate)))
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")
    asyncio.run(run(args.users))


if __name__ == "__main__":
    main()
