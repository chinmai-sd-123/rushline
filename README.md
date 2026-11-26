# Rushline

A seat booking backend built to stay correct under a Tatkal-style rush, plus an AI agent that books seats through the same API. Design: [`docs/design.md`](docs/design.md). Decisions: [`docs/adr/`](docs/adr/).

**Status:** phases 1 and 2 done: the backend core and the AI agent. Next: phase 3, AWS with Terraform.

## What's here

| Area | Built |
|---|---|
| API | FastAPI: `/health`, `/ready`, shows with cursor pagination, seat map, holds |
| Auth | JWT with an `actor` claim (`user`, `agent`, `mcp`), local dev login |
| Data | Postgres 16, hand-written Alembic migrations, constraints that enforce the rules |
| Correctness | Five claim strategies raced in tests, pessimistic locking chosen by benchmark ([ADR 002](docs/adr/002-locking-strategy.md)), idempotency keys |
| Payments | Authorize then capture with a mock provider and injected faults, reconciliation and capture retries ([ADR 003](docs/adr/003-authorize-then-capture.md)) |
| Worker | Hold expiry, payment reconciliation, capture retries, outbox to a Redis stream, `SKIP LOCKED`, graceful shutdown |
| Speed | Seat map cache in Redis with stampede protection, indexes checked against query plans ([plans](docs/query-plans.md)), HTTP load test |
| Agent | LangGraph agent with Postgres checkpoints and a human approval interrupt, tools over the REST API, LLM gateway (OpenAI, fallback, budgets, cost), SSE, chat page |
| Agent safety | Agents pay only with a user's approval, checked by the API ([ADR 004](docs/adr/004-agent-payment-authorization.md)). Isolated `agent` schema and role. Red-team suite and scenario evals in CI |
| RAG and MCP | Venue policies in pgvector with full-text hybrid search and citations, plus an MCP server for browsing and holds |
| Ops | Request ids, one-line JSON logs, CI with real Postgres and Redis |

**Measured so far:**

- **Naive is unsafe:** 500 users racing for 50 seats, 2 each: the naive strategy promised 200 seats to more than one user across 5 trials. Every safe strategy promised 0. [Full results](docs/benchmarks/holds-v1.md).
- **Pessimistic locking is the default:** it had the best safe p50 (1386 ms) with no retries. Serializable was slowest (1906 ms, 1,493 retries). This machine's slow disk dominates the absolute numbers, so compare strategies with each other.
- **The cache holds a launch spike:** at 600 rps of spike traffic the seat map stayed at p99 22.7 ms with 0 errors. Without the cache, the same load gave p99 8.2 s and 29% errors. A cold-cache stampede of 200 requests costs 1 database read. [Load test](docs/benchmarks/load-v3.md).
- **A compromised agent can't pay:** in 7 red-team attacks, a model that obeys injected policy text, guesses approval ids and targets other users' data ends with 0 unsafe actions. The backend refuses every one.
- **No money is lost:** with 10% and 30% of all payment provider calls timing out, there were 0 lost payments, 0 double charges and 0 stuck holds after the worker settled. [Details](docs/benchmarks/payments-v2.md).

## Run it locally

Needs Docker and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env              # then set RUSHLINE_JWT_SECRET to a random string
docker compose up -d --wait       # Postgres 16 + pgvector, Redis 7
uv sync
uv run alembic upgrade head
uv run python -m rushline.seed    # 3 users, 2 shows, 50 seats each
uv run uvicorn rushline.mockpay:app --port 8001 &   # mock payment provider
uv run python -m rushline.worker &                     # expiry, reconciliation, outbox
uv run uvicorn rushline.main:app --reload --no-access-log --timeout-graceful-shutdown 20
```

Try it:

```bash
TOKEN=$(curl -s localhost:8000/auth/dev-token -H 'Content-Type: application/json' \
  -d '{"email":"asha@example.com"}' | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
curl -s localhost:8000/shows
curl -s -X POST localhost:8000/holds -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' -d '{"show_id": 1, "seat_ids": [1, 2]}'
curl -s localhost:8000/shows/1/seats
curl -s -X POST localhost:8000/bookings -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' -d '{"hold_id": 1, "payment_token": "tok_ok"}'
```

`tok_decline` always gets declined. To make the mock provider misbehave, run it with `MOCKPAY_TIMEOUT_RATE=0.1`.

### The agent

```bash
uv run python -m rushline.agent.worker    # needs RUSHLINE_OPENAI_API_KEY in .env
```

Open http://localhost:8000/chat, log in as a demo user and ask, for example, "Book 2 seats for Tatkal Tuesday" or "Can I bring a backpack?". The agent holds seats by itself, but the page asks you to approve before it pays. MCP hosts can run `python -m rushline.mcp_server` with a token from `python -m rushline.auth mcp-token <email>`.

API docs: http://localhost:8000/docs

## Tests

```bash
uv run pytest            # uses a separate rushline_test database
uv run ruff check . && uv run ruff format --check .
```

Tests run against real Postgres, never SQLite, because locking behaviour is the point. Set `RUSHLINE_TEST_FRESH_DB=1` to rebuild the test database from scratch.

## Layout

```
src/rushline/
  main.py           app wiring and lifespan
  config.py         settings from RUSHLINE_* environment variables
  auth.py           JWT, actors, dev login
  shows.py          shows list and seat map
  holds.py          holds and claim strategies
  idempotency.py    Idempotency-Key handling
  bookings.py       authorize-then-capture booking flow
  payments.py       payment provider client (timeouts mean unknown)
  mockpay.py        mock payment provider with fault injection
  worker.py         background jobs
  outbox.py         outbox writes and publisher
  ratelimit.py      per-user limits in Redis
  cache.py          seat map cache with stampede protection
  runs.py           conversations, runs, approvals, live streams
  policies.py       policy ingestion and hybrid search (RAG)
  embeddings.py     OpenAI or offline embeddings
  queue.py          agent job queue (SQS stand-in)
  mcp_server.py     MCP server over the API
  agent/            gateway, tools, LangGraph graph, plain loop, worker
  static/chat.html  the chat page
  health.py         liveness and readiness
  observability.py  request ids and JSON logs
  seed.py           demo data
migrations/         hand-written SQL migrations
data/policies/      venue policies the agent searches
bench/              holds.py (locking), payments.py (reliability), plans.py (query plans), load.py (HTTP load)
tests/              pytest suite against real Postgres
docs/               design doc and ADRs
```
