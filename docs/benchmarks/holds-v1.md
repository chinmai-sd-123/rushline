# Benchmark: seat hold strategies (v1)

Decision record: [ADR 002](../adr/002-locking-strategy.md).

## Run it

```bash
docker compose up -d --wait
uv run python bench/holds.py --trials 5 --clients 500
```

It creates and migrates a separate `rushline_bench` database, so dev data is never touched.

## Method

- **Workload:** 500 users fire `POST /holds` at the same time for show 1, which has 50 seats in 5 rows of 10. Each user asks for 2 adjacent seats in one row, picked from a seeded random generator, so every strategy sees the same requests in the same trial.
- **Users:** each request comes from a different user, so the one-open-hold index never decides the outcome, only seat contention does.
- **App:** the real FastAPI app, run in the same process through an ASGI transport, so there's no network hop. It uses one API process with a 40-connection pool, max overflow 0, and requests queue for connections. Idempotency keys are on, as in production.
- **Database:** Postgres 16 with default durability (`synchronous_commit = on`). Every trial reloads the demo data and runs `VACUUM ANALYZE`, so no trial inherits dead rows from the last.
- **Fairness:** each trial runs every strategy in a freshly shuffled order, which spreads slow drift (disk, background work) across strategies.

**Metrics:**

| Metric | Meaning |
|---|---|
| Double holds | Seats that were in two or more successful holds. Must be 0 |
| Requests/s | All requests answered, divided by wall time |
| Holds/s | Successful holds divided by wall time. Capped by inventory: at most 25 holds fit in 50 seats |
| p50, p99 | Client-side latency of each request, including waiting for a pool connection |
| 409 rate | Requests turned away because a seat was taken |
| Retries | Optimistic re-reads, plus serialization and deadlock retries |
| Deadlocks | From `pg_stat_database` |
| Errors | Anything that wasn't a 201 or a 409 |

## Results (2026-11-26)

Machine: 12 threads, 7.4 GB RAM, Windows 11 with WSL2 (kernel 5.15), Postgres 16 in Docker. This disk is slow to flush: a checkpoint earlier synced files at about 115 ms each.

| Strategy | Double holds | Requests/s | Holds/s | p50 ms | p99 ms | 409 rate | Retries | Deadlocks | Errors |
|---|---|---|---|---|---|---|---|---|---|
| naive | 200 | 233 | 17.6 | 1427 | 1915 | 91% | 0 | 0 | 0 |
| pessimistic | 0 | 228 | 9.4 | 1386 | 1979 | 96% | 0 | 0 | 0 |
| pessimistic_nowait | 0 | 211 | 8.9 | 1620 | 2124 | 96% | 0 | 0 | 0 |
| optimistic | 0 | 223 | 9.4 | 1518 | 2016 | 96% | 136 | 0 | 0 |
| atomic | 0 | 214 | 9.0 | 1424 | 2091 | 96% | 0 | 0 | 0 |
| serializable | 0 | 182 | 7.5 | 1906 | 2520 | 96% | 1493 | 0 | 0 |

5 trials each. Latency and rates are medians across trials. Double holds, retries, deadlocks and errors are totals.

## Caveats

- **Compare strategies with each other, not with real-world capacity.** The client and server share one process and CPU, and latency mostly measures queueing for 40 connections plus slow commits on this disk.
- **Close results are within noise.** Differences of around 10% between pessimistic, atomic and optimistic flipped between earlier runs.
- **Run order matters.** The first version of this benchmark ran strategies one after another and showed later strategies getting steadily slower. That was drift over the run, not the strategies. Interleaving and vacuuming fixed it.
- **One early run hit a real deadlock** in the naive strategy's `UPDATE ... WHERE id = ANY(...)`, which locks rows in physical order. Every strategy now retries deadlock victims.
