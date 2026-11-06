# ADR 004: How the API authorizes agent and MCP payments

Status: Accepted · Date: 2026-11-06

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
- Still open: how the worker gets its short-lived token without putting a bearer token in an SQS message.
