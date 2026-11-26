"""Query plans for every hot query on a production-sized dataset (v3).

Builds 1,000 shows x 500 seats, 100k holds, ~30k bookings and 200k outbox rows in the
separate `rushline_bench` database, then prints EXPLAIN ANALYZE for each query.

    uv run python bench/plans.py            # build data, then explain
    uv run python bench/plans.py --reuse    # explain only
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

ROOT = Path(__file__).resolve().parents[1]
BENCH_DB = "rushline_bench"

BUILD = [
    "DELETE FROM outbox",
    "DELETE FROM idempotency_keys",
    "DELETE FROM bookings",
    "DELETE FROM seats",
    "DELETE FROM holds",
    "DELETE FROM shows",
    "DELETE FROM venues",
    "DELETE FROM users",
    "INSERT INTO users (id, email, name) OVERRIDING SYSTEM VALUE "
    "SELECT g, 'u' || g || '@example.com', 'User' FROM generate_series(1, 50000) g",
    "INSERT INTO venues (id, name, city) OVERRIDING SYSTEM VALUE "
    "SELECT g, 'Venue ' || g, 'Bengaluru' FROM generate_series(1, 10) g",
    "INSERT INTO shows (id, venue_id, title, starts_at) OVERRIDING SYSTEM VALUE "
    "SELECT g, 1 + g % 10, 'Show ' || g, now() + g * interval '1 hour' "
    "FROM generate_series(1, 1000) g",
    # 500 seats per show, ids grouped by show like the real seed.
    "INSERT INTO seats (id, show_id, seat_no, price_paise) OVERRIDING SYSTEM VALUE "
    "SELECT (s - 1) * 500 + n, s, 'S' || n, 20000 + (n % 5) * 10000 "
    "FROM generate_series(1, 1000) s, generate_series(1, 500) n",
    # 100k holds: mostly finished, a few hundred still open (unique user per open hold).
    """INSERT INTO holds (id, user_id, show_id, status, expires_at, payment_deadline)
       OVERRIDING SYSTEM VALUE
       SELECT g, 1 + g % 50000, 1 + g % 1000,
              CASE WHEN g <= 300 THEN 'active' WHEN g <= 400 THEN 'paying'
                   WHEN g % 10 < 6 THEN 'expired' WHEN g % 10 < 9 THEN 'converted'
                   ELSE 'released' END,
              now() - (g % 1000) * interval '1 minute' + interval '5 minutes',
              CASE WHEN g > 300 AND g <= 400 THEN now() + (g % 7 - 3) * interval '1 minute' END
       FROM generate_series(1, 100000) g""",
    """INSERT INTO bookings (user_id, hold_id, amount_paise, status, payment_ref, created_at)
       SELECT user_id, id, 50000,
              CASE WHEN id % 500 = 0 THEN 'pending_capture' ELSE 'confirmed' END,
              'pay_' || id, now() - (id % 10000) * interval '1 minute'
       FROM holds WHERE status = 'converted'""",
    """INSERT INTO outbox (event_type, payload, published_at)
       SELECT 'booking.confirmed', jsonb_build_object('booking_id', g),
              CASE WHEN g <= 199950 THEN now() END
       FROM generate_series(1, 200000) g""",
    # Keep identities ahead of the explicit ids, so later inserts don't collide.
    "SELECT setval(pg_get_serial_sequence('users', 'id'), 50000)",
    "SELECT setval(pg_get_serial_sequence('shows', 'id'), 1000)",
    "SELECT setval(pg_get_serial_sequence('seats', 'id'), 500000)",
    "SELECT setval(pg_get_serial_sequence('holds', 'id'), 100000)",
]

QUERIES = {
    "shows: first page": "SELECT id, venue_id, title, starts_at FROM shows "
    "ORDER BY starts_at, id LIMIT 21",
    "shows: next page (cursor)": "SELECT id, venue_id, title, starts_at FROM shows "
    "WHERE (starts_at, id) > (now() + interval '500 hours', 500) ORDER BY starts_at, id LIMIT 21",
    "seat map (cache miss)": "SELECT id, seat_no, price_paise, status FROM seats "
    "WHERE show_id = 500 ORDER BY id",
    "claim: lock seats": "SELECT status FROM seats WHERE id = ANY('{249501,249502}') "
    "ORDER BY id FOR UPDATE",
    "release: free a hold's seats": "SELECT id FROM seats WHERE hold_id = 42 AND status = 'held'",
    "worker: due expiries": "SELECT id FROM holds WHERE status = 'active' "
    "AND expires_at <= now() ORDER BY expires_at LIMIT 50 FOR UPDATE SKIP LOCKED",
    "worker: due payments": "SELECT id FROM holds WHERE status = 'paying' "
    "AND payment_deadline <= now() ORDER BY payment_deadline LIMIT 50 FOR UPDATE SKIP LOCKED",
    "worker: due captures": "SELECT id FROM bookings WHERE status = 'pending_capture' "
    "AND created_at <= now() ORDER BY created_at LIMIT 50 FOR UPDATE SKIP LOCKED",
    "worker: unpublished outbox": "SELECT id FROM outbox WHERE published_at IS NULL "
    "ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED",
    "GET /bookings (mine)": "SELECT id, hold_id, amount_paise, status, created_at FROM bookings "
    "WHERE user_id = 4242 ORDER BY id DESC LIMIT 100",
}


def bench_url() -> tuple[str, str]:
    sys.path.insert(0, str(ROOT / "src"))
    from rushline.config import Settings

    base = make_url(Settings().database_url)
    os.environ["RUSHLINE_DATABASE_URL"] = base.set(database=BENCH_DB).render_as_string(False)
    return os.environ["RUSHLINE_DATABASE_URL"], base.set(database="postgres").render_as_string(
        False
    )


async def create_database(admin_url: str) -> None:
    admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    async with admin.connect() as conn:
        if not await conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": BENCH_DB}
        ):
            await conn.execute(text(f"CREATE DATABASE {BENCH_DB}"))
    await admin.dispose()


async def build(url: str) -> None:
    engine = create_async_engine(url, poolclass=NullPool)
    async with engine.begin() as conn:
        for sql in BUILD:
            await conn.execute(text(sql))
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("VACUUM ANALYZE"))
    await engine.dispose()


async def explain(url: str) -> None:
    engine = create_async_engine(url, poolclass=NullPool)
    async with engine.connect() as conn:
        for name, sql in QUERIES.items():
            plan = (await conn.execute(text(f"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {sql}"))).all()
            await conn.rollback()  # FOR UPDATE queries take locks: release them
            print(f"\n### {name}\n\n```sql\n{sql}\n```\n\n```")
            print("\n".join(row[0] for row in plan))
            print("```")
    await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reuse", action="store_true", help="skip building the dataset")
    args = parser.parse_args()

    url, admin_url = bench_url()
    asyncio.run(create_database(admin_url))
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")
    if not args.reuse:
        asyncio.run(build(url))
    asyncio.run(explain(url))


if __name__ == "__main__":
    main()
