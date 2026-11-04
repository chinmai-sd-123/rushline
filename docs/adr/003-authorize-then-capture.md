# ADR 003: Authorize then capture, with reconciliation

Status: Accepted · Date: 2026-11-04

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
2. In one transaction: create the booking, mark seats `booked`, mark the hold `converted`.
3. Capture the authorized amount. If step 2 failed, void the authorization instead.

**Reconciliation:** a background job finds holds in `paying` past their deadline, looks up the payment by idempotency key, and either completes the booking or voids the authorization.

## Consequences

- Users are never charged for seats they don't receive. At worst, an authorization is voided and the money never moves.
- Every payment ends in a known state, even after provider timeouts.
- The mock payment service must support authorize, capture, void, lookup by key, and injected timeouts.
- More moving parts: a `paying` state, a payment deadline, and a reconciliation job, all of which need tests.
- This is a compensating-action (saga) approach: each step that can't be done atomically with the database has an undo.
