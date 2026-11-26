"""Seat holds (ADR 001): claim seats for a few minutes before payment.

How seats get claimed is a pluggable strategy (ADR 002). `naive` is broken on purpose:
it reads, then writes, so two concurrent requests can both win the same seat.
"""

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.auth import Principal, require_actor
from rushline.config import Settings
from rushline.deps import get_conn, get_settings

router = APIRouter(prefix="/holds")
any_actor = require_actor("user", "agent", "mcp")  # holds are cheap and expire

ONE_OPEN_HOLD = "holds_one_open_per_user_show"

COUNT_SEATS_IN_SHOW = text(
    "SELECT count(*) FROM seats WHERE show_id = :show_id AND id = ANY(:seat_ids)"
)
INSERT_HOLD = text("""
    INSERT INTO holds (user_id, show_id, status, expires_at)
    VALUES (:user_id, :show_id, 'active', now() + make_interval(secs => :ttl))
    RETURNING id, expires_at
""")
RELEASE_HOLD = text("""
    UPDATE holds SET status = 'released'
    WHERE id = :hold_id AND user_id = :user_id AND status = 'active'
    RETURNING id
""")
HOLD_STATUS = text("SELECT status FROM holds WHERE id = :hold_id AND user_id = :user_id")
FREE_SEATS = text("""
    UPDATE seats SET status = 'available', hold_id = NULL
    WHERE hold_id = :hold_id AND status = 'held'
""")

# --- Claim strategies: return True if every seat was claimed for the hold. ---

ClaimFn = Callable[[AsyncConnection, int, list[int]], Awaitable[bool]]

# Test seam: a race test pauses every racer here, between the read and the write,
# so the double booking reproduces on any machine instead of depending on timing.
after_naive_read: Callable[[], Awaitable[object]] | None = None


async def claim_naive(conn: AsyncConnection, hold_id: int, seat_ids: list[int]) -> bool:
    """Read-then-write. Under READ COMMITTED, both racers see 'available' and both write."""
    statuses = await conn.scalars(
        text("SELECT status FROM seats WHERE id = ANY(:seat_ids)"), {"seat_ids": seat_ids}
    )
    if any(s != "available" for s in statuses):
        return False
    if after_naive_read is not None:
        await after_naive_read()
    await conn.execute(
        text("UPDATE seats SET status = 'held', hold_id = :hold_id WHERE id = ANY(:seat_ids)"),
        {"hold_id": hold_id, "seat_ids": seat_ids},
    )
    return True


STRATEGIES: dict[str, ClaimFn] = {"naive": claim_naive}

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


def _constraint(exc: IntegrityError) -> str | None:
    return getattr(exc.orig.__cause__, "constraint_name", None)


async def create_hold(
    conn: AsyncConnection, settings: Settings, user_id: int, body: HoldIn
) -> HoldOut:
    if len(body.seat_ids) > settings.max_seats_per_hold:
        raise HTTPException(422, f"at most {settings.max_seats_per_hold} seats per hold")
    claim = STRATEGIES[settings.hold_strategy]
    try:
        async with conn.begin():  # any error below rolls back the hold and the claim
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
            if not await claim(conn, hold.id, body.seat_ids):
                raise HTTPException(409, "seat unavailable")
    except IntegrityError as exc:
        if _constraint(exc) == ONE_OPEN_HOLD:
            raise HTTPException(409, "you already have an open hold for this show") from exc
        raise
    return HoldOut(
        hold_id=hold.id, show_id=body.show_id, seat_ids=body.seat_ids, expires_at=hold.expires_at
    )


@router.post("", status_code=status.HTTP_201_CREATED)
async def post_hold(
    body: HoldIn,
    principal: Annotated[Principal, Depends(any_actor)],
    settings: Annotated[Settings, Depends(get_settings)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> HoldOut:
    return await create_hold(conn, settings, principal.user_id, body)


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
