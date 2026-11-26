"""Seat holds (ADR 001): claim seats for a few minutes before payment.

How seats get claimed is a strategy (ADR 002). `naive` reads then writes and is broken
on purpose; the others are the safe candidates the benchmark compares.
"""

from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline import idempotency
from rushline.auth import Principal, require_actor
from rushline.config import Settings
from rushline.deps import get_conn, get_settings

router = APIRouter(prefix="/holds")
any_actor = require_actor("user", "agent", "mcp")  # holds are cheap and expire

ONE_OPEN_HOLD = "holds_one_open_per_user_show"
LOCK_NOT_AVAILABLE = "55P03"
SERIALIZATION_FAILURE = "40001"
DEADLOCK_DETECTED = "40P01"
RETRYABLE = {SERIALIZATION_FAILURE: "serialization_retries", DEADLOCK_DETECTED: "deadlock_retries"}
MAX_ATTEMPTS = 3  # optimistic re-reads, and whole-transaction retries

stats: Counter[str] = Counter()  # retries, read by the benchmark

# Test seams: race tests pause every racer here so contention doesn't depend on timing.
before_claim: Callable[[], Awaitable[object]] | None = None
after_naive_read: Callable[[], Awaitable[object]] | None = None

COUNT_SEATS_IN_SHOW = text(
    "SELECT count(*) FROM seats WHERE show_id = :show_id AND id = ANY(:seat_ids)"
)
INSERT_HOLD = text("""
    INSERT INTO holds (user_id, show_id, status, expires_at)
    VALUES (:user_id, :show_id, 'active', now() + make_interval(secs => :ttl))
    RETURNING id, expires_at
""")
MARK_HELD = text("UPDATE seats SET status = 'held', hold_id = :hold_id WHERE id = ANY(:seat_ids)")
RELEASE_HOLD = text("""
    UPDATE holds SET status = 'released'
    WHERE id = :hold_id AND user_id = :user_id AND status = 'active'
    RETURNING id
""")
OPTIMISTIC_CLAIM = text("""
    UPDATE seats s
    SET status = 'held', hold_id = :hold_id, version = s.version + 1
    FROM unnest(CAST(:ids AS bigint[]), CAST(:versions AS int[])) AS v(id, version)
    WHERE s.id = v.id AND s.version = v.version
    RETURNING s.id
""")
HOLD_STATUS = text("SELECT status FROM holds WHERE id = :hold_id AND user_id = :user_id")
FREE_SEATS = text("""
    UPDATE seats SET status = 'available', hold_id = NULL
    WHERE hold_id = :hold_id AND status = 'held'
""")

# --- Claim strategies: return True only if every seat was claimed for the hold. ---

ClaimFn = Callable[[AsyncConnection, int, list[int]], Awaitable[bool]]


async def claim_naive(conn: AsyncConnection, hold_id: int, seat_ids: list[int]) -> bool:
    """Read, then write. Two racers can both read 'available' and both write."""
    statuses = await conn.scalars(
        text("SELECT status FROM seats WHERE id = ANY(:seat_ids)"), {"seat_ids": seat_ids}
    )
    if any(s != "available" for s in statuses):
        return False
    if after_naive_read is not None:
        await after_naive_read()
    await conn.execute(MARK_HELD, {"hold_id": hold_id, "seat_ids": seat_ids})
    return True


async def _claim_locked(
    conn: AsyncConnection, hold_id: int, seat_ids: list[int], nowait: bool
) -> bool:
    # Lock in id order so two requests can never wait on each other in a cycle.
    lock = "FOR UPDATE NOWAIT" if nowait else "FOR UPDATE"
    try:
        statuses = await conn.scalars(
            text(f"SELECT status FROM seats WHERE id = ANY(:seat_ids) ORDER BY id {lock}"),
            {"seat_ids": seat_ids},
        )
    except DBAPIError as exc:
        if nowait and exc.orig.sqlstate == LOCK_NOT_AVAILABLE:
            return False  # someone else is claiming these seats right now
        raise
    if any(s != "available" for s in statuses):
        return False
    await conn.execute(MARK_HELD, {"hold_id": hold_id, "seat_ids": seat_ids})
    return True


async def claim_pessimistic(conn: AsyncConnection, hold_id: int, seat_ids: list[int]) -> bool:
    """Lock the rows, then check and write. Losers wait for the winner to commit."""
    return await _claim_locked(conn, hold_id, seat_ids, nowait=False)


async def claim_pessimistic_nowait(
    conn: AsyncConnection, hold_id: int, seat_ids: list[int]
) -> bool:
    """Like pessimistic, but losers fail at once instead of queueing."""
    return await _claim_locked(conn, hold_id, seat_ids, nowait=True)


class _VersionConflict(Exception):
    pass


async def claim_optimistic(conn: AsyncConnection, hold_id: int, seat_ids: list[int]) -> bool:
    """Read versions without locking; write only if no version changed, else re-read."""
    for _ in range(MAX_ATTEMPTS):
        rows = (
            await conn.execute(
                text("SELECT id, status, version FROM seats WHERE id = ANY(:seat_ids)"),
                {"seat_ids": seat_ids},
            )
        ).all()
        if any(r.status != "available" for r in rows):
            return False
        try:
            async with conn.begin_nested():  # savepoint: a partial write is undone on conflict
                claimed = await conn.execute(
                    OPTIMISTIC_CLAIM,
                    {
                        "hold_id": hold_id,
                        "ids": [r.id for r in rows],
                        "versions": [r.version for r in rows],
                    },
                )
                if len(claimed.all()) != len(seat_ids):
                    raise _VersionConflict
            return True
        except _VersionConflict:
            stats["optimistic_retries"] += 1
    return False


async def claim_atomic(conn: AsyncConnection, hold_id: int, seat_ids: list[int]) -> bool:
    """One conditional UPDATE: claims only seats still available, all or nothing."""
    claimed = (
        await conn.execute(
            text("""
                UPDATE seats SET status = 'held', hold_id = :hold_id
                WHERE id = ANY(:seat_ids) AND status = 'available'
                RETURNING id
            """),
            {"hold_id": hold_id, "seat_ids": seat_ids},
        )
    ).all()
    return len(claimed) == len(seat_ids)  # a partial claim is rolled back by the caller


@dataclass(frozen=True)
class Strategy:
    claim: ClaimFn
    serializable: bool = False  # run the whole transaction at SERIALIZABLE, retrying


STRATEGIES: dict[str, Strategy] = {
    "naive": Strategy(claim_naive),
    "pessimistic": Strategy(claim_pessimistic),
    "pessimistic_nowait": Strategy(claim_pessimistic_nowait),
    "optimistic": Strategy(claim_optimistic),
    "atomic": Strategy(claim_atomic),
    "serializable": Strategy(claim_naive, serializable=True),
}

# --- API ---


class HoldIn(BaseModel):
    show_id: int
    seat_ids: list[int] = Field(min_length=1)

    @field_validator("seat_ids")
    @classmethod
    def no_duplicates(cls, seat_ids: list[int]) -> list[int]:
        if len(set(seat_ids)) != len(seat_ids):
            raise ValueError("duplicate seat ids")
        return seat_ids


class HoldOut(BaseModel):
    hold_id: int
    show_id: int
    seat_ids: list[int]
    expires_at: datetime


Finalize = Callable[[AsyncConnection, HoldOut], Awaitable[None]]


def _constraint(exc: IntegrityError) -> str | None:
    return getattr(exc.orig.__cause__, "constraint_name", None)


async def _hold_once(
    conn: AsyncConnection,
    settings: Settings,
    strategy: Strategy,
    user_id: int,
    body: HoldIn,
    finalize: Finalize | None,
) -> HoldOut:
    async with conn.begin():  # any error below rolls back the hold and the claim
        if strategy.serializable:
            await conn.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
        found = await conn.scalar(
            COUNT_SEATS_IN_SHOW, {"show_id": body.show_id, "seat_ids": body.seat_ids}
        )
        if found != len(body.seat_ids):
            raise HTTPException(422, "unknown seats for this show")
        hold = (
            await conn.execute(
                INSERT_HOLD,
                {"user_id": user_id, "show_id": body.show_id, "ttl": settings.hold_ttl_seconds},
            )
        ).one()
        if before_claim is not None:
            await before_claim()
        if not await strategy.claim(conn, hold.id, body.seat_ids):
            raise HTTPException(409, "seat unavailable")
        out = HoldOut(
            hold_id=hold.id,
            show_id=body.show_id,
            seat_ids=body.seat_ids,
            expires_at=hold.expires_at,
        )
        if finalize is not None:
            await finalize(conn, out)  # e.g. store the idempotent response atomically
        return out


async def create_hold(
    conn: AsyncConnection,
    settings: Settings,
    user_id: int,
    body: HoldIn,
    finalize: Finalize | None = None,
) -> HoldOut:
    if len(body.seat_ids) > settings.max_seats_per_hold:
        raise HTTPException(422, f"at most {settings.max_seats_per_hold} seats per hold")
    strategy = STRATEGIES[settings.hold_strategy]
    for attempt in range(MAX_ATTEMPTS):
        try:
            return await _hold_once(conn, settings, strategy, user_id, body, finalize)
        except IntegrityError as exc:
            if _constraint(exc) == ONE_OPEN_HOLD:
                raise HTTPException(409, "you already have an open hold for this show") from exc
            raise
        except DBAPIError as exc:
            # Postgres rolled the whole transaction back, so running it again is safe.
            counter = RETRYABLE.get(getattr(exc.orig, "sqlstate", None))
            if counter is None:
                raise
            if attempt == MAX_ATTEMPTS - 1:
                raise HTTPException(409, "too much contention, try again") from exc
            stats[counter] += 1
    raise AssertionError("unreachable")


@router.post("", status_code=status.HTTP_201_CREATED, response_model=HoldOut)
async def post_hold(
    body: HoldIn,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    principal: Annotated[Principal, Depends(any_actor)],
    settings: Annotated[Settings, Depends(get_settings)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
):
    key = idempotency.Key(principal.user_id, idempotency_key)
    replay = await idempotency.claim(conn, key, idempotency.request_hash("POST /holds", body))
    if replay is not None:
        return replay

    async def store(c: AsyncConnection, out: HoldOut) -> None:
        await idempotency.store(c, key, 201, out.model_dump(mode="json"))

    try:
        return await create_hold(conn, settings, principal.user_id, body, finalize=store)
    except HTTPException as exc:
        await idempotency.store_outside(conn, key, exc.status_code, {"detail": exc.detail})
        raise
    except Exception:
        await idempotency.release(conn, key)  # let the client retry with the same key
        raise


@router.delete("/{hold_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_hold(
    hold_id: int,
    principal: Annotated[Principal, Depends(any_actor)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> Response:
    params = {"hold_id": hold_id, "user_id": principal.user_id}
    async with conn.begin():
        if (await conn.execute(RELEASE_HOLD, params)).first() is not None:
            await conn.execute(FREE_SEATS, params)
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        current = await conn.scalar(HOLD_STATUS, params)
    # 404 for other users' holds too, so ids can't be probed.
    if current is None:
        raise HTTPException(404, "hold not found")
    raise HTTPException(409, f"hold is {current}")
