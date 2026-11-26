# Rushline

A seat booking backend built to stay correct under a Tatkal-style rush, plus an AI agent that books seats through the same API. Design: [`docs/design.md`](docs/design.md). Decisions: [`docs/adr/`](docs/adr/).

**Status:** v0 done (foundation and a reproduced double-booking race). Next: v1, safe locking strategies and a benchmark.

## What's here

| Area | Built |
|---|---|
| API | FastAPI: `/health`, `/ready`, shows with cursor pagination, seat map, holds |
| Auth | JWT with an `actor` claim (`user`, `agent`, `mcp`), local dev login |
| Data | Postgres 16, hand-written Alembic migrations, constraints that enforce the rules |
| Correctness | A test that reproduces double booking with the naive strategy |
| Ops | Request ids, one-line JSON logs, CI with real Postgres and Redis |

**Measured so far:** with the naive strategy, 20 users requesting the same 2 seats at once got 3 to 11 successful holds per run on the dev machine (median 7, 20 runs). Whether it happens by itself depends on timing, so the test forces the worst interleaving and every racer wins. That's the bug v1 fixes and benchmarks.

## Run it locally

Needs Docker and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env              # then set RUSHLINE_JWT_SECRET to a random string
docker compose up -d --wait       # Postgres 16 + pgvector, Redis 7
uv sync
uv run alembic upgrade head
uv run python -m rushline.seed    # 3 users, 2 shows, 50 seats each
uv run uvicorn rushline.main:app --reload --no-access-log
```

Try it:

```bash
TOKEN=$(curl -s localhost:8000/auth/dev-token -H 'Content-Type: application/json' \
  -d '{"email":"asha@example.com"}' | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
curl -s localhost:8000/shows
curl -s -X POST localhost:8000/holds -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"show_id": 1, "seat_ids": [1, 2]}'
curl -s localhost:8000/shows/1/seats
```

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
  health.py         liveness and readiness
  observability.py  request ids and JSON logs
  seed.py           demo data
migrations/         hand-written SQL migrations
tests/              pytest suite against real Postgres
docs/               design doc and ADRs
```
