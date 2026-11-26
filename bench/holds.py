"""Hold benchmark (ADR 002): N users, 50 seats, 2 adjacent seats each, all at once.

Runs the real app in-process (ASGI, no network) against a separate `rushline_bench`
database with normal durability. Every strategy sees the same seat requests per trial,
strategies are interleaved in shuffled order, and tables are vacuumed before each run.

    uv run python bench/holds.py --trials 3 --clients 500
"""

import argparse
import asyncio
import os
import random
import statistics
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
STRATEGIES = ["naive", "pessimistic", "pessimistic_nowait", "optimistic", "atomic", "serializable"]


def configure_env() -> str:
    """Point the app at the bench database before it's imported."""
    sys.path.insert(0, str(ROOT / "src"))
    from rushline.config import Settings

    base = make_url(Settings().database_url)
    os.environ["RUSHLINE_DATABASE_URL"] = base.set(database=BENCH_DB).render_as_string(False)
    os.environ["RUSHLINE_DB_POOL_SIZE"] = "40"  # one API copy's pool; requests queue for it
    os.environ["RUSHLINE_DB_MAX_OVERFLOW"] = "0"
    os.environ["RUSHLINE_DB_POOL_TIMEOUT_SECONDS"] = "120"
    os.environ["RUSHLINE_LOG_LEVEL"] = "WARNING"
    return base.set(database="postgres").render_as_string(False)


async def create_database(admin_url: str) -> None:
    admin = create_async_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    async with admin.connect() as conn:
        exists = await conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": BENCH_DB}
        )
        if not exists:
            await conn.execute(text(f"CREATE DATABASE {BENCH_DB}"))
    await admin.dispose()


async def reset(clients: int) -> list[int]:
    from rushline.seed import load_demo_data

    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.begin() as conn:
        await load_demo_data(conn)  # show 1 has seats 1..50, 10 per row
        users = await conn.scalars(
            text(
                "INSERT INTO users (email, name) "
                "SELECT 'bench' || g || '@example.com', 'Bench' FROM generate_series(1, :n) g "
                "RETURNING id"
            ),
            {"n": clients},
        )
        user_ids = list(users)
    # Same starting point every run: no dead rows left over from earlier trials.
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("VACUUM ANALYZE"))
    await engine.dispose()
    return user_ids


async def deadlocks() -> int:
    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.connect() as conn:
        count = await conn.scalar(
            text("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
        )
    await engine.dispose()
    return count


def adjacent_pairs(clients: int, seed: int) -> list[list[int]]:
    rng = random.Random(seed)
    pairs = [[s, s + 1] for s in range(1, 50) if s % 10 != 0]  # never across a row end
    return [rng.choice(pairs) for _ in range(clients)]


async def trial(strategy: str, clients: int, seed: int) -> dict:
    import httpx2

    from rushline import holds
    from rushline.auth import create_token
    from rushline.config import Settings
    from rushline.main import app

    os.environ["RUSHLINE_HOLD_STRATEGY"] = strategy
    user_ids = await reset(clients)
    requests = adjacent_pairs(clients, seed)
    settings = Settings()
    headers = [{"Authorization": f"Bearer {create_token(settings, u)}"} for u in user_ids]
    holds.stats.clear()
    deadlocks_before = await deadlocks()

    async with app.router.lifespan_context(app):
        # Count server errors (e.g. a deadlock victim) instead of crashing the run.
        transport = httpx2.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx2.AsyncClient(
            transport=transport, base_url="http://bench", timeout=300
        ) as client:

            async def one(i: int) -> tuple[int, float, list[int]]:
                start = time.perf_counter()
                r = await client.post(
                    "/holds",
                    json={"show_id": 1, "seat_ids": requests[i]},
                    headers={**headers[i], "Idempotency-Key": uuid.uuid4().hex},
                )
                return r.status_code, time.perf_counter() - start, requests[i]

            wall = time.perf_counter()
            results = await asyncio.gather(*(one(i) for i in range(clients)))
            wall = time.perf_counter() - wall

    await asyncio.sleep(1.5)  # let Postgres flush its stats counters
    codes = Counter(code for code, _, _ in results)
    claimed = [seat for code, _, seats in results if code == 201 for seat in seats]
    latencies = sorted(lat for _, lat, _ in results)
    return {
        "double_holds": len(claimed) - len(set(claimed)),  # seats promised to 2+ users
        "holds_per_s": codes[201] / wall,
        "requests_per_s": clients / wall,
        "p50_ms": statistics.median(latencies) * 1000,
        "p99_ms": latencies[int(len(latencies) * 0.99) - 1] * 1000,
        "rate_409": codes[409] / clients,
        "errors": clients - codes[201] - codes[409],
        "retries": sum(holds.stats.values()),
        "deadlocks": await deadlocks() - deadlocks_before,
    }


def summarize(strategy: str, runs: list[dict]) -> str:
    med = {k: statistics.median(r[k] for r in runs) for k in runs[0]}
    total = {k: sum(r[k] for r in runs) for k in ("double_holds", "errors", "retries", "deadlocks")}
    return (
        f"| {strategy} | {total['double_holds']} | {med['requests_per_s']:.0f} | "
        f"{med['holds_per_s']:.1f} | "
        f"{med['p50_ms']:.0f} | {med['p99_ms']:.0f} | {med['rate_409']:.0%} | "
        f"{total['retries']} | {total['deadlocks']} | {total['errors']} |"
    )


async def run(args: argparse.Namespace) -> None:
    print(
        "| Strategy | Double holds | Requests/s | Holds/s | p50 ms | p99 ms | 409 rate "
        "| Retries | Deadlocks | Errors |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    # Interleave strategies in a shuffled order per trial, so drift over the run
    # (disk, autovacuum) doesn't favour whichever strategy ran first.
    runs: dict[str, list[dict]] = {s: [] for s in args.strategies}
    for t in range(args.trials):
        order = random.Random(1000 + t).sample(args.strategies, len(args.strategies))
        for strategy in order:
            runs[strategy].append(await trial(strategy, args.clients, seed=t))
    for strategy in args.strategies:
        print(summarize(strategy, runs[strategy]), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--clients", type=int, default=500)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--strategies", nargs="+", default=STRATEGIES, choices=STRATEGIES)
    args = parser.parse_args()

    asyncio.run(create_database(configure_env()))
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")  # runs its own event loop
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
