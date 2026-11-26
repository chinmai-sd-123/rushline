# ADR 004: How the API authorizes agent and MCP payments

Status: Accepted · Date: 2026-11-06 · Implemented: 2026-11-26

## Context

The agent may create holds by itself, but payment needs explicit user approval, checked server-side. The first design gave the agent the user's own token. The API then couldn't tell agent from user, so it had no way to require approval only for the agent.

The risk is prompt injection. Hidden text in a venue policy could tell the agent to pay now. We have to assume this can fool the LLM, so the protection has to live in the API.

MCP hosts have the same problem, and they have no approval screen of ours.

## Decision

**Tokens carry an actor claim:**

| Actor | Who | Payment |
|---|---|---|
| `user` | The person, through the chat page or direct API | Can pay and decide approvals |
| `agent` | Agent worker. Short-lived and tied to one `run_id` | Can pay only with a valid approval. Can't decide approvals |
| `mcp` | MCP server, one token per user | Can't pay, cancel bookings or decide approvals |

**An approval covers one exact payment:** it's tied to one `hold_id` and amount, can be used once, and expires no later than the hold.

**Only a `user` token can decide an approval.** Otherwise, the agent could approve itself.

**The agent never sees the payment token.** The user gives it to the API at approval time. It's stored on the approval, cleared once used or expired, and never returned by an API.

The approval is marked used in the same transaction that moves the hold to `paying`, so two requests can't both use it.

## Alternatives considered

| Option | Rejected because |
|---|---|
| User's own token for the agent | The API can't tell who is calling. |
| Enforce approval in the agent or in prompts | A fooled agent would skip it. Rules live in the backend. |
| Approval that covers "pay" in general | The agent could use it for a different hold or amount. |
| Payment token passed through the agent | It could leak into prompts, logs and LLM provider requests. |
| Shared MCP service account | The API couldn't tell whose booking it was touching. |
| Payment through MCP hosts | We can't show or verify an approval step in someone else's host. Payment stays in our chat page or direct API. |

## Consequences

- A fully fooled agent still can't pay, because the API rejects it without an approval only the user can give.
- More to build and test: the actor claim, approval checks, and a red-team test for each rejection.
- The approval row briefly stores a payment token, so it must stay out of logs. This is a mock token today, and a real provider would need stronger handling.
- MCP users have to finish payment in the chat page.
- **Resolved: how the worker gets its token.** The agent worker is trusted backend code that holds the signing key. It mints a 15-minute `actor=agent` token for the one run it's working on, so jobs carry only `run_id` and `user_id`, never a bearer token.

## Implementation

- **Tokens:** `src/rushline/auth.py` carries the `actor` claim, and `require_actor(...)` is set on every route.
- **Approvals:** `src/rushline/runs.py`.
  - `POST /approvals` is for agents only. It reads the amount and expiry from the hold.
  - `POST /runs/{id}/approvals/{aid}` is for users only. It stores the payment token and queues `run.resume`.
  - A partial unique index allows one live approval per hold.
  - A check constraint keeps the payment token only while the approval is `approved`.
- **Paying:** `POST /bookings` takes `approval_id` from agents and `payment_token` from users. `use_approval` consumes the approval, returns its token once and clears it, all in the same transaction that flips the hold to `paying`.
- **The agent cannot pay by construction:** it has no tool that approves anything, and its tools call the API with its own token.

## Verified

- **`tests/test_agent_api.py`:**
  - no payment without an approval
  - a pending approval is not enough
  - an agent can't approve its own payment
  - an approval is bound to its run
  - approvals are single use, expire with the hold, and allow one live per hold
  - MCP tokens can't pay or request approvals
- **`tests/test_redteam.py`:** a fully compromised model that obeys injected policy text, guesses approval ids, aims at another user's booking, hoards seats, injects arguments and loops ends with 0 payments and 0 changes to other users' data.
