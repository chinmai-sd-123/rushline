# ADR 002: Locking strategy for seat holds

Status: Proposed, pending benchmark · Date: 2026-11-04

## Context

Creating a hold must claim several seats all-or-nothing while hundreds of requests compete for the same rows. A naive read-then-write under Postgres's default READ COMMITTED isolation lets two users hold the same seat.

Candidates:

| Strategy | How | Expected strength | Expected weakness |
|---|---|---|---|
| Pessimistic | `SELECT ... ORDER BY id FOR UPDATE`, then UPDATE | Simple, safe, no retries | Requests queue behind hot seats |
| Pessimistic NOWAIT | Same with `FOR UPDATE NOWAIT` | Losers fail fast | More 409s/errors to handle |
| Optimistic | `version` column, `UPDATE ... WHERE version = :v` | No lock waiting | Retry storms under high contention |
| Atomic conditional update | `UPDATE ... WHERE id = ANY(:ids) AND status = 'available' RETURNING id` | One round trip, shortest lock time | Logic limited to what fits in a WHERE clause |
| SERIALIZABLE (optional) | Naive code at SERIALIZABLE isolation, retry on serialization failure | Correct without manual locking | Retry storms like optimistic |

## Decision

To be decided by benchmark, not opinion.

Benchmark: 500 concurrent clients, each a different user, 50 seats, each requesting 2 adjacent seats. Different users matter because the one-open-hold index (ADR 001) would otherwise reject requests before they compete for seats, which would distort the results.

Results (fill in after running):

| Strategy | Double holds | Holds/s | p50 ms | p99 ms | 409 rate | Retries | Deadlocks |
|---|---|---|---|---|---|---|---|
| Naive (broken baseline) | | | | | | | |
| Pessimistic | | | | | | | |
| Pessimistic NOWAIT | | | | | | | |
| Optimistic | | | | | | | |
| Atomic conditional | | | | | | | |

Chosen strategy: _TBD_

## Consequences

_To be written after the benchmark: why the winner won, and what we gave up._
