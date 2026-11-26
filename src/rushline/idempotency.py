"""Idempotency keys: a retried POST gets the first response back instead of running twice.

Flow: claim the key in its own short transaction (so a concurrent duplicate sees it),
do the work, and store the response inside the work's transaction (so a crash can't
leave a hold without its stored response).
"""

import hashlib
import json
from dataclasses import dataclass

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

STALE_SECONDS = 30  # an unanswered claim this old belongs to a request that crashed

CLAIM = text("""
    INSERT INTO idempotency_keys (user_id, key, request_hash)
    VALUES (:user_id, :key, :hash)
    ON CONFLICT (user_id, key) DO NOTHING
    RETURNING 1
""")
EXISTING = text("""
    SELECT request_hash, response_status, response_body,
           created_at < now() - make_interval(secs => :stale) AS stale
    FROM idempotency_keys
    WHERE user_id = :user_id AND key = :key
    FOR UPDATE
""")
RECLAIM = text(
    "UPDATE idempotency_keys SET created_at = now() WHERE user_id = :user_id AND key = :key"
)
STORE = text("""
    UPDATE idempotency_keys
    SET response_status = :status, response_body = CAST(:body AS jsonb)
    WHERE user_id = :user_id AND key = :key
""")
RELEASE = text("""
    DELETE FROM idempotency_keys
    WHERE user_id = :user_id AND key = :key AND response_status IS NULL
""")

IN_PROGRESS = "a request with this Idempotency-Key is still in progress"


@dataclass(frozen=True)
class Key:
    user_id: int  # keys are per user, so users can't collide or see each other's responses
    value: str


def request_hash(route: str, body: BaseModel) -> str:
    return hashlib.sha256(f"{route}\n{body.model_dump_json()}".encode()).hexdigest()


def _params(key: Key, **extra) -> dict:
    return {"user_id": key.user_id, "key": key.value, **extra}


async def claim(conn: AsyncConnection, key: Key, req_hash: str) -> JSONResponse | None:
    """Reserve the key. Returns the stored response to replay, or None to go ahead."""
    async with conn.begin():
        if await conn.scalar(CLAIM, _params(key, hash=req_hash)):
            return None
        row = (await conn.execute(EXISTING, _params(key, stale=STALE_SECONDS))).one_or_none()
        if row is None:  # released between our insert and read: let the client retry
            raise HTTPException(409, IN_PROGRESS)
        if row.request_hash != req_hash:
            raise HTTPException(422, "Idempotency-Key was already used for a different request")
        if row.response_status is not None:
            return JSONResponse(
                row.response_body,
                status_code=row.response_status,
                headers={"Idempotent-Replayed": "true"},
            )
        if not row.stale:
            raise HTTPException(409, IN_PROGRESS)
        await conn.execute(RECLAIM, _params(key))  # the first attempt died mid-flight
        return None


async def store(conn: AsyncConnection, key: Key, status: int, body: object) -> None:
    """Save the response inside the caller's transaction."""
    await conn.execute(STORE, _params(key, status=status, body=json.dumps(body)))


async def store_outside(conn: AsyncConnection, key: Key, status: int, body: object) -> None:
    """After the work failed: keep client errors (a retry would fail the same way),
    but free the key on server errors so the client can retry."""
    if status >= 500:
        await release(conn, key)
        return
    async with conn.begin():
        await store(conn, key, status, body)


async def release(conn: AsyncConnection, key: Key) -> None:
    async with conn.begin():
        await conn.execute(RELEASE, _params(key))
