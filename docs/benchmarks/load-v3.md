# Load test: seat map cache under a launch spike (v3)

The "Scale" proof from the design doc's rollout plan. It measures a real server over HTTP, with the seat map cache on and off.

## Setup

**Dataset:** `bench/plans.py`, with 1,000 shows, 500,000 seats and 50,000 users.

**Server:** uvicorn with 4 worker processes on `rushline_bench`. Each worker has a 10-connection pool, and the seat map cache is either on or off:

```bash
RUSHLINE_DATABASE_URL=postgresql+asyncpg://rushline:rushline@localhost:5432/rushline_bench \
RUSHLINE_SEAT_MAP_CACHE=true RUSHLINE_LOG_LEVEL=WARNING RUSHLINE_HOLDS_PER_MINUTE=100000 \
uv run uvicorn rushline.main:app --port 8765 --workers 4 --no-access-log
```

**Client:** `bench/load.py` sends open-loop traffic at a fixed rate for 30 seconds, split across 2 to 6 client processes on the same machine:

```bash
uv run python bench/load.py --rps 600 --seconds 30 --processes 4 --hot-share 0.9
```

**Traffic mix:** 90% seat maps, 8% show listings, 2% holds on random seats by random users.

| Mode | Where seat map reads go |
|---|---|
| Launch spike | 90% to one show, the rest spread across all 1,000 |
| Uniform | Spread evenly across all 1,000 shows. Worst case for a cache |

**Machine:** the same dev machine as [holds-v1](holds-v1.md): 12 threads, WSL2, Postgres 16 in Docker with a slow-to-flush disk. The client and server share it.

## Results (2026-11-26)

### Seat map (design target: p99 < 200 ms)

| Traffic | Rate sent | Cache | p50 ms | p95 ms | p99 ms | Errors | From Redis |
|---|---|---|---|---|---|---|---|
| Uniform | 300 rps | on | 5.2 | 36.4 | 118.3 | 0 | 84% |
| Uniform | 300 rps | off | 8.6 | 38.0 | 84.4 | 0 | 0% |
| Spike | 300 rps | on | 3.6 | 11.1 | 15.5 | 0 | 94% |
| Spike | 300 rps | off | 8.9 | 42.0 | 66.5 | 0 | 0% |
| **Spike** | **600 rps** | **on** | **3.8** | **12.1** | **22.7** | **0** | 94% |
| Spike | 600 rps | off | 5,084 | 6,950 | 8,197 | 29% (503) | 0% |
| Spike | 900 rps | on | 8.1 | 178.2 | 1,007.5 | 0 | 95% |
| Spike | 900 rps | off | 10,019 | 20,456 | 23,986 | 57% (503 and timeouts) | 0% |

The client kept to its schedule in every run with the cache on, so those numbers measure the server. With the cache off at 600 and 900 rps, the server fell behind: it actually served 482 and 552 rps.

### Holds (writes, never cached)

| Traffic | Rate sent | Cache | p50 ms | p99 ms | Outcome |
|---|---|---|---|---|---|
| Spike | 600 rps | on | 61.6 | 241.1 | 382 of 382 held |
| Spike | 600 rps | off | 2,918.7 | 9,237.9 | 84 got 503, 2 timed out |
| Spike | 900 rps | on | 122.0 | 1,932.7 | 560 held, 2 seats already taken |

## What it shows

- **Under a spike, the cache is the difference between working and not.** At 600 rps the cached seat map stays at p99 22.7 ms with no errors. Uncached, the database pool runs out and 29% of reads get a 503.
- **The cache protects writes too.** Holds share the same database. With reads served from Redis, holds kept their connections: p50 62 ms at 600 rps, against 2.9 s with the cache off.
- **The ceiling on this machine is about 900 rps of spike traffic.** Seat map p99 reaches about 1 s there, still with 0 errors. Uncached, the ceiling is below 500 rps.
- **Uniform traffic is the cache's worst case.** Each of 1,000 shows is read about every 2 seconds, longer than the 1-second fresh window. Stale-while-revalidate still served 84% from Redis, but there's no latency win at 300 rps.
- **Overload is a 503, never a crash.** Every overloaded request got a retryable 503 or a client timeout.

## Things the load test found

- **Tokens rejected by clock skew.** The client's token was rejected as "not yet valid", because this machine's clock steps backwards. The API now allows 30 seconds of skew.
- **Seeds replaying old requests.** The load generator reused the same random seed every run, so it replayed earlier runs' users and seats, and holds failed with 409s that said nothing about the API. Each run now gets a new seed.
- **One client process isn't enough.** A single Python client process can't send much more than 300 to 400 rps. The client now runs in several processes and reports how many requests it sent late.

## Caveats

- **The client and server share one machine,** and its disk is slow to flush, so absolute numbers would differ on separate machines or RDS.
- **The server uses 4 processes.** There's no load balancer, and only one copy of the API.
- **Holds land on random seats,** so they rarely compete with each other. Contention under load is measured separately in [holds-v1](holds-v1.md).
