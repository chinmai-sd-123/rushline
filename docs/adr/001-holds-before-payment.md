# ADR 001: Hold seats before payment

Status: Accepted · Date: 2026-11-04

## Context

Booking a seat involves a payment call to an external provider that takes 1 to 5 seconds and can fail or time out. During a rush, hundreds of users compete for the same seats.

If we book in a single step, we must either:
- hold a database row lock for the whole payment call, which queues every other request for that seat for seconds and exhausts connections during a spike, or
- check availability, charge, then write, which lets two users both get charged for one seat.

## Decision

Split booking into two steps:

1. **Hold:** a short transaction marks seats as held by the user for 10 minutes. All contention is resolved here.
2. **Book:** payment runs against an existing hold, outside any seat lock.

A background worker releases holds that expire unpaid. Hold duration (10 minutes) and max seats per hold (6) are configuration values, not constants in code.

The agent may create holds without asking the user, because holds are cheap and expire. Payment always requires explicit user approval.

## Consequences

- Contention is confined to a fast step (milliseconds), so the slow payment call never blocks other users.
- Matches how users already expect ticketing to work.
- Fits the agent flow: it can hold seats, then ask for approval while the seats are safe.
- We need an expiry worker and must handle the race between expiry and payment (see ADR 003).
- Holds can be abused to hoard seats. Mitigated by backend limits: max 6 seats per hold, one open hold per user per show (a partial unique index on `holds`, counting `active` and `paying`), rate limits.
- To change seats, a user must release their hold first, and may lose those seats to someone else in that moment. An "add to hold" flow could come later.
