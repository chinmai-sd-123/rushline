# Payment reliability check (v2)

Decision record: [ADR 003](../adr/003-authorize-then-capture.md). This is the "Reliability" proof from the design doc's rollout plan.

## Run it

```bash
docker compose up -d --wait
uv run python bench/payments.py --users 100 --timeout-rate 0.1
```

## Method

- **Workload:** 100 users each hold one different seat and pay, all at once, through the real app in-process. There's no seat contention here, only payment trouble.
- **Faults:** the mock provider times out on the given share of **every** call (authorize, capture, void, lookup). Half the timeouts happen before the work ("never happened"), half after ("happened, but nobody heard"). The client gives up after 0.2s, and the payment window is 1s.
- **Settling:** the worker ticks until no hold is `paying` and no booking is `pending_capture`.
- **The checks compare our database with the provider's records:**

| Check | Meaning |
|---|---|
| Lost payments | Captured at the provider, but no confirmed booking. Must be 0 |
| Free seats | Confirmed booking, but no captured payment. Must be 0 |
| Double charges | More than one payment for one hold. Must be 0 |
| Holds left paying, authorizations left open | Must be 0 |

## Results (2026-11-26, dev machine as in [holds-v1](holds-v1.md))

| Timeouts | Responses | Settled after | Converted / released | Lost | Free seats | Double charges | Stuck |
|---|---|---|---|---|---|---|---|
| 10% | 87 × 201, 13 × 202 | 4 ticks (9.7s) | 94 / 6 | 0 | 0 | 0 | 0 |
| 10% | 91 × 201, 9 × 202 | 2 ticks (7.2s) | 94 / 6 | 0 | 0 | 0 | 0 |
| 30% | 48 × 201, 23 × 202, 29 × 503 on hold | 3 ticks (8.6s) | 55 / 16 | 0 | 0 | 0 | 0 |

- **Released holds** are ones whose authorize never reached the provider. Nothing was charged for them.
- **The 503s at 30%:** with this many slow requests on this machine's slow disk, the 40-connection pool ran out. Those requests got a retryable 503 instead of a hold.

## What it found

The first runs showed some **500s**. The cause was the database pool timing out under load, and that error had no handler and no log line. The API now answers **503 with `Retry-After`** when the pool is exhausted or Postgres is down, and logs every unhandled error with its request id. There's a test for both cases in `tests/test_overload.py`.
