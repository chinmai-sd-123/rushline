# ADR 002: Locking strategy for seat holds

Status: Accepted · Date: 2026-11-26 (proposed 2026-11-04)

## Context

Creating a hold must claim several seats all-or-nothing while hundreds of requests compete for the same rows. A naive read-then-write under Postgres's default READ COMMITTED isolation lets two users hold the same seat.

Candidates, all in `src/rushline/holds.py` and selectable with `RUSHLINE_HOLD_STRATEGY`:

| Strategy | How |
|---|---|
| Naive (broken baseline) | `SELECT status`, then `UPDATE` if all were available |
| Pessimistic | `SELECT ... ORDER BY id FOR UPDATE`, then check and `UPDATE` |
| Pessimistic NOWAIT | Same with `FOR UPDATE NOWAIT`; a locked seat fails at once |
| Optimistic | Read `version`, `UPDATE ... WHERE version = :v` in a savepoint, re-read on conflict |
| Atomic conditional update | `UPDATE ... WHERE id = ANY(:ids) AND status = 'available' RETURNING id`, all or nothing |
| Serializable | Naive code at SERIALIZABLE isolation, retrying serialization failures |

## Decision

**Pessimistic locking with `ORDER BY id FOR UPDATE` is the default.** The other strategies stay in the code so the comparison can be rerun.

Every strategy also retries a deadlock victim (SQLSTATE `40P01`) up to 3 times, because Postgres rolls the victim's whole transaction back.

## Evidence

### Correctness

`tests/test_race.py` pauses 10 users at the same point and releases them together, so contention doesn't depend on timing:

- Naive: all 10 users get a 201 for the same 2 seats.
- Every other strategy: exactly 1 gets a 201, the other 9 get a 409.

### Benchmark

`bench/holds.py`: 500 users at once, each asking for 2 adjacent seats out of 50. The same requests are used for every strategy, which run interleaved in shuffled order, 5 trials each, with tables vacuumed before every run. Latencies are medians across trials. Double holds, retries, deadlocks and errors are totals.

| Strategy | Double holds | Requests/s | Holds/s | p50 ms | p99 ms | 409 rate | Retries | Deadlocks | Errors |
|---|---|---|---|---|---|---|---|---|---|
| Naive (broken baseline) | **200** | 233 | 17.6 | 1427 | 1915 | 91% | 0 | 0 | 0 |
| **Pessimistic** | 0 | 228 | 9.4 | **1386** | 1979 | 96% | 0 | 0 | 0 |
| Pessimistic NOWAIT | 0 | 211 | 8.9 | 1620 | 2124 | 96% | 0 | 0 | 0 |
| Optimistic | 0 | 223 | 9.4 | 1518 | 2016 | 96% | 136 | 0 | 0 |
| Atomic conditional | 0 | 214 | 9.0 | 1424 | 2091 | 96% | 0 | 0 | 0 |
| Serializable | 0 | 182 | 7.5 | 1906 | 2520 | 96% | 1493 | 0 | 0 |

Measured 2026-11-26 on the dev machine: 12 threads, 7.4 GB RAM, Windows 11 with WSL2, Postgres 16 in Docker with default durability (`synchronous_commit = on`), one API process with a 40-connection pool. Full method and caveats: [`docs/benchmarks/holds-v1.md`](../benchmarks/holds-v1.md).

### How to read it

- **Naive is unsafe.** It "won" 200 seats that had already been promised to someone else, across 5 trials. That's also why it looks faster: it said yes when it should have said no.
- **The four lock-based safe strategies are close.** Their p50s are within about 15% of each other, and on this machine the latency is dominated by waiting for a pool connection and by slow disk flushes on commit. The ranking between pessimistic, atomic and optimistic is within run-to-run noise.
- **Serializable is clearly worst.** It needed 1,493 retries, and its p50 was 37% higher than pessimistic.
- **Optimistic retries under contention** (136), as expected.

## Why pessimistic

- **Correct:** 0 double holds in tests and the benchmark.
- **Fastest safe p50** in this run, with no retries.
- **Deadlock-free between claims by construction:** every claim locks its seats in id order, so two claims can never wait on each other in a cycle. Atomic's `UPDATE ... WHERE id = ANY(...)` locks rows in physical order, which drifts from id order as rows are updated. An early benchmark run hit exactly that deadlock in the naive strategy's identical `UPDATE`.
- **Easy to reason about and explain:** lock, check, write.

## Consequences

- Losers wait for the winner's transaction to commit instead of failing at once. A hold transaction is a few milliseconds, so the wait is short, but it does count against latency during a spike.
- A claim takes two statements (lock, then update) instead of atomic's one. That would matter more with a remote database, where each round trip costs network time, so recheck against RDS if it's ever deployed.
- Other writers to `seats` (the expiry worker, bookings) may lock in a different order. The deadlock retry covers that.
- NOWAIT stays available for a future "fail fast" mode, for example if the agent should give up quickly instead of queueing.
