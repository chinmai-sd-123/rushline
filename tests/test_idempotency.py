import asyncio
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


def post(client, headers, key, seat_ids=(1, 2)):
    return client.post(
        "/holds",
        json={"show_id": 1, "seat_ids": list(seat_ids)},
        headers={**headers, "Idempotency-Key": key},
    )


def run_sql(sql: str, **params):
    async def go():
        engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params)
            rows = result.all() if result.returns_rows else []
        await engine.dispose()
        return rows

    return asyncio.run(go())


def active_holds() -> int:
    return run_sql("SELECT count(*) FROM holds WHERE status = 'active'")[0][0]


def test_missing_key_is_422(client, auth):
    response = client.post("/holds", json={"show_id": 1, "seat_ids": [1]}, headers=auth())
    assert response.status_code == 422


def test_retry_replays_the_first_response_and_holds_once(client, auth):
    first = post(client, auth(), "k1")
    retry = post(client, auth(), "k1")

    assert first.status_code == retry.status_code == 201
    assert retry.json() == first.json()
    assert retry.headers["idempotent-replayed"] == "true"
    assert active_holds() == 1


def test_client_errors_are_replayed_too(client, auth):
    post(client, auth(2), "other-user", seat_ids=[1])
    first = post(client, auth(), "k1")
    run_sql("UPDATE seats SET status = 'available', hold_id = NULL")  # seats free up...

    retry = post(client, auth(), "k1")

    assert first.status_code == retry.status_code == 409  # ...but the key's answer stands
    assert retry.json() == {"detail": "seat unavailable"}


def test_same_key_for_a_different_request_is_422(client, auth):
    post(client, auth(), "k1", seat_ids=[1])

    response = post(client, auth(), "k1", seat_ids=[2])

    assert response.status_code == 422
    assert response.json() == {"detail": "Idempotency-Key was already used for a different request"}


def test_keys_are_per_user(client, auth):
    post(client, auth(1), "shared", seat_ids=[1])

    response = post(client, auth(2), "shared", seat_ids=[2])

    assert response.status_code == 201
    assert "idempotent-replayed" not in response.headers


def test_duplicate_while_first_is_in_progress_is_409(client, auth):
    first = post(client, auth(), "k1")
    run_sql("UPDATE idempotency_keys SET response_status = NULL, response_body = NULL")

    response = post(client, auth(), "k1")

    assert first.status_code == 201
    assert response.status_code == 409
    assert response.json() == {"detail": "a request with this Idempotency-Key is still in progress"}


def test_abandoned_claim_is_taken_over_after_it_goes_stale(client, auth):
    # A request claimed the key, then crashed before doing anything.
    from rushline.holds import HoldIn
    from rushline.idempotency import request_hash

    body_hash = request_hash("POST /holds", HoldIn(show_id=1, seat_ids=[1, 2]))
    run_sql(
        "INSERT INTO idempotency_keys (user_id, key, request_hash, created_at) "
        "VALUES (1, 'k1', :hash, now() - interval '1 minute')",
        hash=body_hash,
    )

    response = post(client, auth(), "k1")

    assert response.status_code == 201
    assert active_holds() == 1
