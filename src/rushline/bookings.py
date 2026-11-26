"""Bookings: pay for a hold with authorize-then-capture (ADR 003).

1. Flip the hold active -> paying (410 if it expired). The expiry worker ignores paying holds.
2. Authorize the amount with the provider, keyed by the hold so it can be looked up later.
3. One transaction: booking as pending_capture, seats booked, hold converted.
4. Capture, then mark the booking confirmed.
If authorize times out, the hold stays paying and the worker reconciles it after
the payment deadline. If capture doesn't finish, the worker retries it.
"""

import logging
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline import idempotency, outbox
from rushline.auth import Principal, require_actor
from rushline.config import Settings
from rushline.deps import get_conn, get_settings
from rushline.payments import Payment, PaymentClient, PaymentDeclined, PaymentUnknown
from rushline.ratelimit import rate_limit
from rushline.runs import use_approval

log = logging.getLogger(__name__)
router = APIRouter(prefix="/bookings")

START_PAYMENT = text("""
    UPDATE holds
    SET status = 'paying', payment_deadline = now() + make_interval(secs => :window)
    WHERE id = :hold_id AND user_id = :user_id AND status = 'active' AND expires_at > now()
    RETURNING id
""")
HOLD_STATE = text("""
    SELECT status, expires_at <= now() AS lapsed FROM holds
    WHERE id = :hold_id AND user_id = :user_id
""")
HOLD_AMOUNT = text(
    "SELECT coalesce(sum(price_paise), 0) FROM seats WHERE hold_id = :hold_id AND status = 'held'"
)
CONVERT_HOLD = text("""
    UPDATE holds SET status = 'converted'
    WHERE id = :hold_id AND status = 'paying'
    RETURNING user_id
""")
BOOK_SEATS = text("UPDATE seats SET status = 'booked' WHERE hold_id = :hold_id AND status = 'held'")
INSERT_BOOKING = text("""
    INSERT INTO bookings (user_id, hold_id, amount_paise, payment_ref)
    VALUES (:user_id, :hold_id, :amount, :payment_ref)
    RETURNING id
""")
CONFIRM = text("""
    UPDATE bookings SET status = 'confirmed'
    WHERE id = :booking_id AND status = 'pending_capture'
    RETURNING hold_id
""")
RELEASE_PAYING = text("""
    UPDATE holds SET status = 'released'
    WHERE id = :hold_id AND status = 'paying'
    RETURNING id
""")
FREE_HELD_SEATS = text("""
    UPDATE seats SET status = 'available', hold_id = NULL
    WHERE hold_id = :hold_id AND status = 'held'
""")
CANCEL = text("""
    UPDATE bookings SET status = 'cancelled'
    WHERE id = :booking_id AND user_id = :user_id AND status = 'confirmed'
    RETURNING hold_id
""")
FREE_BOOKED_SEATS = text("""
    UPDATE seats SET status = 'available', hold_id = NULL
    WHERE hold_id = :hold_id AND status = 'booked'
""")
BOOKING_STATUS = text("SELECT status FROM bookings WHERE id = :booking_id AND user_id = :user_id")
MY_BOOKINGS = text("""
    SELECT id AS booking_id, hold_id, amount_paise, status, created_at
    FROM bookings WHERE user_id = :user_id ORDER BY id DESC LIMIT 100
""")


def payment_key(hold_id: int) -> str:
    """One provider key per hold, so a retry or the reconciler finds the same payment."""
    return f"hold-{hold_id}"


# --- Steps shared with the worker. Each runs inside the caller's transaction. ---


async def complete_booking(conn: AsyncConnection, hold_id: int, payment: Payment) -> int | None:
    """Paying hold -> converted, seats -> booked, booking -> pending_capture.

    Returns the booking id, or None if the hold is no longer paying (someone else settled it).
    """
    user_id = await conn.scalar(CONVERT_HOLD, {"hold_id": hold_id})
    if user_id is None:
        return None
    await conn.execute(BOOK_SEATS, {"hold_id": hold_id})
    booking_id = await conn.scalar(
        INSERT_BOOKING,
        {
            "user_id": user_id,
            "hold_id": hold_id,
            "amount": payment.amount_paise,
            "payment_ref": payment.id,
        },
    )
    await outbox.add(conn, "booking.created", {"booking_id": booking_id, "hold_id": hold_id})
    return booking_id


async def confirm_booking(conn: AsyncConnection, booking_id: int) -> bool:
    hold_id = await conn.scalar(CONFIRM, {"booking_id": booking_id})
    if hold_id is not None:
        await outbox.add(conn, "booking.confirmed", {"booking_id": booking_id, "hold_id": hold_id})
    return hold_id is not None


async def release_paying_hold(conn: AsyncConnection, hold_id: int) -> bool:
    released = await conn.scalar(RELEASE_PAYING, {"hold_id": hold_id})
    if released is not None:
        await conn.execute(FREE_HELD_SEATS, {"hold_id": hold_id})
    return released is not None


# --- API ---


class BookingIn(BaseModel):
    """Users pay with a payment token. Agents pay only with an approval (ADR 004)."""

    hold_id: int
    payment_token: str | None = Field(default=None, min_length=1, max_length=200)
    approval_id: int | None = None

    @model_validator(mode="after")
    def one_way_to_pay(self) -> "BookingIn":
        if (self.payment_token is None) == (self.approval_id is None):
            raise ValueError("send exactly one of payment_token or approval_id")
        return self


class BookingOut(BaseModel):
    booking_id: int | None  # None while the payment outcome is unknown (202)
    hold_id: int
    amount_paise: int
    status: Literal["processing", "pending_capture", "confirmed"]


class BookingSummary(BaseModel):
    booking_id: int
    hold_id: int
    amount_paise: int
    status: str
    created_at: datetime


async def _start_payment(
    conn: AsyncConnection, settings: Settings, principal: Principal, body: BookingIn
) -> tuple[int, str]:
    """Step 1. Returns (amount, payment token). An agent's approval is used up here, in
    the same transaction that flips the hold, so a failed flip leaves it unused."""
    params = {"hold_id": body.hold_id, "user_id": principal.user_id}
    async with conn.begin():
        token = body.payment_token
        approved_amount = None
        if principal.actor == "agent":
            approval = await use_approval(conn, principal, body.approval_id, body.hold_id)
            if approval is None:
                raise HTTPException(403, "no valid approval for this hold")
            token, approved_amount = approval
        started = await conn.scalar(
            START_PAYMENT, {**params, "window": settings.payment_window_seconds}
        )
        if started is not None:
            amount = await conn.scalar(HOLD_AMOUNT, params)
            if approved_amount is not None and amount != approved_amount:
                raise HTTPException(409, "hold amount differs from the approved amount")
            return amount, token
        hold = (await conn.execute(HOLD_STATE, params)).one_or_none()
    if hold is None:
        raise HTTPException(404, "hold not found")
    if hold.status == "expired" or (hold.status == "active" and hold.lapsed):
        raise HTTPException(410, "hold expired")
    raise HTTPException(409, f"hold is {hold.status}")


async def book(
    conn: AsyncConnection,
    payments: PaymentClient,
    settings: Settings,
    principal: Principal,
    body: BookingIn,
    key: idempotency.Key,
) -> tuple[int, BookingOut]:
    amount, token = await _start_payment(conn, settings, principal, body)

    try:
        payment = await payments.authorize(payment_key(body.hold_id), amount, token)
    except PaymentDeclined as exc:
        async with conn.begin():
            await release_paying_hold(conn, body.hold_id)
        raise HTTPException(402, "payment declined") from exc
    except PaymentUnknown:
        log.warning("authorize outcome unknown", extra={"fields": {"hold_id": body.hold_id}})
        out = BookingOut(
            booking_id=None, hold_id=body.hold_id, amount_paise=amount, status="processing"
        )
        async with conn.begin():
            await idempotency.store(conn, key, 202, out.model_dump(mode="json"))
        return 202, out  # the worker settles it after the payment deadline

    async with conn.begin():
        booking_id = await complete_booking(conn, body.hold_id, payment)
        if booking_id is not None:
            out = BookingOut(
                booking_id=booking_id,
                hold_id=body.hold_id,
                amount_paise=payment.amount_paise,
                status="pending_capture",
            )
            await idempotency.store(conn, key, 201, out.model_dump(mode="json"))
    if booking_id is None:  # the hold was settled elsewhere: don't keep the money
        await _void_quietly(payments, payment)
        raise HTTPException(409, "hold is no longer awaiting payment")

    try:
        await payments.capture(payment.id)
    except Exception:  # unknown or rejected: the worker retries pending captures
        log.warning("capture deferred", extra={"fields": {"booking_id": booking_id}})
        return 201, out
    async with conn.begin():
        await confirm_booking(conn, booking_id)
        out = out.model_copy(update={"status": "confirmed"})
        await idempotency.store(conn, key, 201, out.model_dump(mode="json"))
    return 201, out


async def _void_quietly(payments: PaymentClient, payment: Payment) -> None:
    try:
        await payments.void(payment.id)
    except Exception:  # an unvoided authorization expires on its own at the provider
        log.exception("void failed", extra={"fields": {"payment_id": payment.id}})


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=BookingOut,
    dependencies=[Depends(rate_limit("bookings"))],
)
async def post_booking(
    body: BookingIn,
    request: Request,
    response: Response,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    # Users pay with a token; agents only with a user's approval (ADR 004); MCP never.
    principal: Annotated[Principal, Depends(require_actor("user", "agent"))],
    settings: Annotated[Settings, Depends(get_settings)],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
):
    if (principal.actor == "agent") != (body.approval_id is not None):
        raise HTTPException(403, "users pay with a payment token, agents with an approval")
    key = idempotency.Key(principal.user_id, idempotency_key)
    replay = await idempotency.claim(conn, key, idempotency.request_hash("POST /bookings", body))
    if replay is not None:
        return replay
    try:
        code, out = await book(conn, request.app.state.payments, settings, principal, body, key)
    except HTTPException as exc:
        await idempotency.store_outside(conn, key, exc.status_code, {"detail": exc.detail})
        raise
    except Exception:
        await idempotency.release(conn, key)
        raise
    response.status_code = code
    return out


@router.get("")
async def list_bookings(
    principal: Annotated[Principal, Depends(require_actor("user", "agent"))],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> list[BookingSummary]:
    rows = (await conn.execute(MY_BOOKINGS, {"user_id": principal.user_id})).mappings()
    return [BookingSummary(**row) for row in rows]


@router.delete("/{booking_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_booking(
    booking_id: int,
    principal: Annotated[Principal, Depends(require_actor("user", "agent"))],
    conn: Annotated[AsyncConnection, Depends(get_conn)],
) -> Response:
    """Soft cancel: seats go back on sale. Refunds are a non-goal."""
    params = {"booking_id": booking_id, "user_id": principal.user_id}
    async with conn.begin():
        hold_id = await conn.scalar(CANCEL, params)
        if hold_id is not None:
            await conn.execute(FREE_BOOKED_SEATS, {"hold_id": hold_id})
            await outbox.add(conn, "booking.cancelled", {"booking_id": booking_id})
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        current = await conn.scalar(BOOKING_STATUS, params)
    if current is None:
        raise HTTPException(404, "booking not found")
    raise HTTPException(409, f"booking is {current}")
