# ADR 003: Authorize then capture, with reconciliation

Status: Accepted · Date: 2026-11-04 · Amended: 2026-11-06 (added `pending_capture` and capture retries) · Implemented: 2026-11-26

## Context

A user can start paying just as their hold expires, and someone else may then hold the same seat. Worse, the payment provider can time out, leaving us not knowing whether the user was charged. Without care, we could take money for a seat the user doesn't get, or lose track of a payment entirely.

Preventing the expiry race is possible. Preventing network uncertainty is not: a timeout can always leave the payment result unknown.

## Decision

**Prevention:** starting payment atomically moves the hold from `active` to `paying`, only if it hasn't expired:

```sql
UPDATE holds SET status = 'paying', payment_deadline = now() + interval '2 minutes'
WHERE id = :id AND status = 'active' AND expires_at > now()
RETURNING id;
```

Zero rows returns 410 and nothing is charged. The expiry worker ignores `paying` holds.

**Two-step payment:**
1. Authorize the amount with the provider, sending our idempotency key.
2. In one transaction: create the booking as `pending_capture`, mark seats `booked`, mark the hold `converted`.
3. Capture the authorized amount, then mark the booking `confirmed`. If step 2 failed, void the authorization instead.

**Reconciliation:** a background job finds holds in `paying` past their deadline, looks up the payment by idempotency key, and either completes the booking or voids the authorization.

**Capture retries:** the same job finds bookings stuck in `pending_capture` and retries capture with the same key. If retries run out, for example because the authorization expired, the booking becomes `capture_failed` and raises an alert.

Why this was added: in the original order, a crash after step 2 left a `converted` hold with no capture. Reconciliation only looks at `paying` holds, so it would never find it, and the user would keep the seat without paying.

Why not capture before saving: if the save then failed, money would have moved for a seat the user didn't get, which needs a refund. Refunds are a non-goal, and a stuck `pending_capture` row is safer and easier to recover.

## Implementation (v2)

- **API:** `POST /bookings` in `src/rushline/bookings.py`. The provider key is `hold-{hold_id}`, so a retry or the reconciler always finds the same payment.
- **Unknown authorize:** a timeout or 5xx answers **202** `processing` and leaves the hold `paying`. The client checks `GET /holds/{id}` or `GET /bookings`.
- **Decline:** answers **402** and releases the hold immediately.
- **Worker:** `src/rushline/worker.py` (`uv run python -m rushline.worker`) handles reconciliation and capture retries, claiming rows with `FOR UPDATE SKIP LOCKED`. A lookup that times out is retried on the next tick, and the hold stays `paying` until the provider answers. Capture gives up after `RUSHLINE_CAPTURE_MAX_ATTEMPTS` unknowns, or immediately on a definite rejection such as an expired authorization.
- **Every state change** writes an outbox event in the same transaction.

## Verified

- **Tests** (`tests/test_bookings.py`, `tests/test_worker.py`):
  - decline
  - expired hold
  - authorize timeout after the work, which the worker books
  - authorize timeout before the work, which the worker releases with nothing charged
  - a crash between saving and capturing, which ends `confirmed`
  - an expired authorization, which ends `capture_failed`
  - capture retries exhausted
  - an unreachable provider during reconciliation
- **Reliability check** (`bench/payments.py`, [results](../benchmarks/payments-v2.md)): with 10% and 30% of all provider calls timing out, there were 0 lost payments, 0 free seats, 0 double charges and 0 holds left `paying`.

## Consequences

- Users are never charged for seats they don't receive. At worst, an authorization is voided and the money never moves.
- Every payment ends in a known state, even after provider timeouts or a crash before capture. `capture_failed` is the one state that needs a person to look at it.
- The mock payment service must support authorize, capture, void, lookup by key, and injected timeouts. Capture must be idempotent.
- More moving parts: a `paying` state, a payment deadline, a `pending_capture` state, and a reconciliation job, all of which need tests.
- A booking can't be cancelled while `pending_capture`, because capture may still be in progress.
- This is a compensating-action (saga) approach: each step that can't be done atomically with the database has an undo.
