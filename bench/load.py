"""HTTP load test against a running API (v3): open-loop traffic at a fixed rate.

Start the API on the bench dataset first (see docs/benchmarks/load-v3.md), then:

    uv run python bench/load.py --url http://127.0.0.1:8765 --rps 300 --seconds 30 --processes 3

One Python client process tops out around 300-400 rps, so higher rates are split across
processes and the results merged. "Sent late" counts requests the client couldn't send on
schedule: if it's not near 0, the client, not the API, was the bottleneck.

The mix follows the design's numbers: reads dominate. 90% seat maps, 8% show
listings, 2% holds on random seats by random users. With --hot-share 0.9, 90% of
seat map reads go to show 1, like a launch spike; otherwise shows are uniform.
"""

import argparse
import asyncio
import random
import statistics
import sys
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import httpx2

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from rushline.auth import create_token  # noqa: E402
from rushline.config import Settings  # noqa: E402

SHOWS, SEATS_PER_SHOW, USERS = 1000, 500, 50_000  # bench/plans.py dataset


def pick(rng: random.Random, settings: Settings, hot_share: float) -> tuple[str, str, dict]:
    """One request from the mix: (name, path, kwargs)."""
    roll = rng.random()
    if roll < 0.90:
        show = 1 if rng.random() < hot_share else rng.randint(1, SHOWS)
        return "seat map", f"/shows/{show}/seats", {}
    if roll < 0.98:
        return "shows", "/shows?limit=20", {}
    show = rng.randint(1, SHOWS)
    seat = (show - 1) * SEATS_PER_SHOW + rng.randint(1, SEATS_PER_SHOW)
    token = create_token(settings, rng.randint(1, USERS))
    return (
        "hold",
        "/holds",
        {
            "method": "POST",
            "json": {"show_id": show, "seat_ids": [seat]},
            "headers": {"Authorization": f"Bearer {token}", "Idempotency-Key": uuid.uuid4().hex},
        },
    )


async def run(
    url: str, rps: int, seconds: int, seed: int, hot_share: float
) -> tuple[dict, Counter, int]:
    settings, rng = Settings(), random.Random(seed)
    latencies: dict[str, list[float]] = defaultdict(list)
    codes: Counter = Counter()
    late = 0
    limits = httpx2.Limits(max_connections=500, max_keepalive_connections=500)
    async with httpx2.AsyncClient(base_url=url, timeout=10, limits=limits) as client:

        async def one(name: str, path: str, kwargs: dict) -> None:
            method = kwargs.pop("method", "GET")
            start = time.perf_counter()
            try:
                response = await client.request(method, path, **kwargs)
                codes[(name, response.status_code)] += 1
                if "x-cache" in response.headers:
                    codes[("cache", response.headers["x-cache"])] += 1
            except httpx2.HTTPError as exc:
                codes[(name, type(exc).__name__)] += 1
            latencies[name].append(time.perf_counter() - start)

        tasks, begin = [], time.perf_counter()
        for i in range(rps * seconds):
            # Open loop: send on schedule whether or not earlier requests finished.
            delay = begin + i / rps - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            elif delay < -0.1:
                late += 1  # the client itself fell behind schedule
            tasks.append(asyncio.create_task(one(*pick(rng, settings, hot_share))))
        await asyncio.gather(*tasks)
    return dict(latencies), codes, late


def run_process(args: tuple[str, int, int, int, float]) -> tuple[dict, Counter, int]:
    return asyncio.run(run(*args))


def report(rps: int, seconds: int, results: list, wall: float) -> None:
    latencies: dict[str, list[float]] = defaultdict(list)
    codes: Counter = Counter()
    late = 0
    for lat, cod, lt in results:
        for name, values in lat.items():
            latencies[name].extend(values)
        codes.update(cod)
        late += lt
    total = sum(len(v) for v in latencies.values())
    print(
        f"target {rps} rps for {seconds}s: sent {total} in {wall:.1f}s "
        f"= {total / wall:.0f} rps achieved, {late} sent late"
    )
    print("| Endpoint | Requests | p50 ms | p95 ms | p99 ms | Status codes |")
    print("|---|---|---|---|---|---|")
    for name in ("seat map", "shows", "hold"):
        lat = sorted(latencies[name])
        if not lat:
            continue
        q = statistics.quantiles(lat, n=100)
        status = ", ".join(f"{c}: {n}" for (e, c), n in sorted(codes.items(), key=str) if e == name)
        print(
            f"| {name} | {len(lat)} | {q[49] * 1000:.1f} | {q[94] * 1000:.1f} | "
            f"{q[98] * 1000:.1f} | {status} |"
        )
    cache = {c: n for (e, c), n in codes.items() if e == "cache"}
    if cache:
        hits = cache.get("hit", 0) + cache.get("stale", 0)
        print(f"\nseat map cache: {cache}, served from Redis: {hits / sum(cache.values()):.0%}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--rps", type=int, default=300)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--hot-share", type=float, default=0.0, help="seat map reads on show 1")
    # A new seed per run by default: reusing one replays the same users and seats as the
    # last run, so holds fail with 409s that say nothing about the API.
    parser.add_argument("--seed", type=int, default=int(time.time()))
    args = parser.parse_args()

    share = args.rps // args.processes
    jobs = [
        (args.url, share, args.seconds, args.seed * 100 + i, args.hot_share)
        for i in range(args.processes)
    ]
    begin = time.perf_counter()
    with ProcessPoolExecutor(args.processes) as pool:
        results = list(pool.map(run_process, jobs))
    report(share * args.processes, args.seconds, results, time.perf_counter() - begin)


if __name__ == "__main__":
    main()
