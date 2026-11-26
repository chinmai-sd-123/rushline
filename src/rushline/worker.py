"""Background worker: expires holds, settles unknown payments, retries captures and
publishes the outbox. Every job claims rows with FOR UPDATE SKIP LOCKED, so several
worker copies can run side by side without doing the same work twice.

Run: uv run python -m rushline.worker   (stops cleanly on SIGTERM or Ctrl+C)
"""

import asyncio
import logging
import signal

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from rushline.bookings import (
    complete_booking,
    confirm_booking,
    payment_key,
    release_paying_hold,
)
from rushline.config import Settings
from rushline.db import create_engine
from rushline.observability import configure_logging
from rushline.outbox import RedisStreamPublisher
from rushline.payments import PaymentClient, PaymentRejected, PaymentUnknown

log = logging.getLogger("rushline.worker")

EXPIRE_HOLDS = text("""
    WITH due AS (
        SELECT id FROM holds
        WHERE status = 'active' AND expires_at <= now()
        ORDER BY expires_at
        LIMIT :batch
        FOR UPDATE SKIP LOCKED
    )
    UPDATE holds h SET status = 'expired' FROM due WHERE h.id = due.id
    RETURNING h.id
""")
FREE_SEATS = text("""
    UPDATE seats SET status = 'available', hold_id = NULL
    WHERE hold_id = ANY(:hold_ids) AND status = 'held'
""")
DUE_PAYMENTS = text("""
    SELECT id FROM holds
    WHERE status = 'paying' AND payment_deadline <= now()
    ORDER BY payment_deadline
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
""")
DUE_CAPTURES = text("""
    SELECT id, payment_ref, capture_attempts FROM bookings
    WHERE status = 'pending_capture' AND created_at <= now() - make_interval(secs => :min_age)
    ORDER BY created_at
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
""")
CAPTURE_TRIED = text("""
    UPDATE bookings SET capture_attempts = capture_attempts + 1
    WHERE id = :id
    RETURNING capture_attempts
""")
CAPTURE_FAILED = text(
    "UPDATE bookings SET status = 'capture_failed' WHERE id = :id AND status = 'pending_capture'"
)
DUE_EVENTS = text("""
    SELECT id, event_type, payload FROM outbox
    WHERE published_at IS NULL
    ORDER BY id
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
""")
MARK_PUBLISHED = text("UPDATE outbox SET published_at = now() WHERE id = ANY(:ids)")


async def expire_holds(engine: AsyncEngine, batch: int) -> int:
    """Active holds past expires_at release their seats. Paying holds are never touched."""
    async with engine.begin() as conn:
        expired = list(await conn.scalars(EXPIRE_HOLDS, {"batch": batch}))
        if expired:
            await conn.execute(FREE_SEATS, {"hold_ids": expired})
    return len(expired)


async def reconcile_payments(engine: AsyncEngine, payments: PaymentClient, batch: int) -> int:
    """Paying holds past their deadline: ask the provider what really happened.

    Rows stay locked during the lookups, so the API can't settle the same hold at once.
    """
    settled = 0
    async with engine.begin() as conn:
        for hold_id in list(await conn.scalars(DUE_PAYMENTS, {"batch": batch})):
            try:
                payment = await payments.lookup(payment_key(hold_id))
            except PaymentUnknown:
                continue  # provider still unreachable: try again next tick
            if payment is not None and payment.status in ("authorized", "captured"):
                booking_id = await complete_booking(conn, hold_id, payment)
                if booking_id is not None and payment.status == "captured":
                    await confirm_booking(conn, booking_id)
                log.info("payment reconciled: booked", extra={"fields": {"hold_id": hold_id}})
            else:  # never authorized, declined or voided: nothing was charged
                await release_paying_hold(conn, hold_id)
                log.info("payment reconciled: released", extra={"fields": {"hold_id": hold_id}})
            settled += 1
    return settled


async def retry_captures(engine: AsyncEngine, payments: PaymentClient, settings: Settings) -> int:
    """Bookings stuck in pending_capture (e.g. a crash after saving). Capture is idempotent."""
    done = 0
    async with engine.begin() as conn:
        due = (
            await conn.execute(
                DUE_CAPTURES,
                {
                    "batch": settings.worker_batch_size,
                    "min_age": settings.capture_retry_after_seconds,
                },
            )
        ).all()
        for booking in due:
            try:
                await payments.capture(booking.payment_ref)
            except PaymentUnknown:
                attempts = await conn.scalar(CAPTURE_TRIED, {"id": booking.id})
                if attempts < settings.capture_max_attempts:
                    continue
                reason = "retries exhausted"
            except PaymentRejected as exc:
                reason = str(exc)  # a definite no, e.g. the authorization expired
            else:
                await confirm_booking(conn, booking.id)
                done += 1
                continue
            await conn.execute(CAPTURE_FAILED, {"id": booking.id})
            log.error(  # needs a person: seats are booked but money wasn't captured
                "capture failed",
                extra={"fields": {"booking_id": booking.id, "reason": reason}},
            )
    return done


async def publish_outbox(engine: AsyncEngine, publisher: RedisStreamPublisher, batch: int) -> int:
    """At least once: a crash after publishing but before marking means a re-send."""
    async with engine.begin() as conn:
        events = (await conn.execute(DUE_EVENTS, {"batch": batch})).all()
        for event in events:
            await publisher.publish(event.id, event.event_type, event.payload)
        if events:
            await conn.execute(MARK_PUBLISHED, {"ids": [e.id for e in events]})
    return len(events)


async def tick(
    engine: AsyncEngine,
    payments: PaymentClient,
    publisher: RedisStreamPublisher,
    settings: Settings,
) -> dict[str, int]:
    batch = settings.worker_batch_size
    return {
        "expired": await expire_holds(engine, batch),
        "reconciled": await reconcile_payments(engine, payments, batch),
        "captured": await retry_captures(engine, payments, settings),
        "published": await publish_outbox(engine, publisher, batch),
    }


async def run(stop: asyncio.Event, settings: Settings | None = None) -> None:
    """Loop until `stop` is set. The current tick always finishes first."""
    settings = settings or Settings()
    engine = create_engine(settings)
    redis = Redis.from_url(settings.redis_url, socket_timeout=2, socket_connect_timeout=2)
    payments = PaymentClient(settings.payment_url, settings.payment_timeout_seconds)
    publisher = RedisStreamPublisher(redis)
    log.info("worker started")
    try:
        while not stop.is_set():
            try:
                counts = await tick(engine, payments, publisher, settings)
                if any(counts.values()):
                    log.info("tick", extra={"fields": counts})
            except Exception:  # one bad tick (e.g. Redis down) must not kill the worker
                log.exception("tick failed")
            try:
                await asyncio.wait_for(stop.wait(), settings.worker_interval_seconds)
            except TimeoutError:
                pass
    finally:
        await payments.aclose()
        await redis.aclose()
        await engine.dispose()
        log.info("worker stopped")


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    async def go() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)  # graceful: finish the tick, then exit
        await run(stop, settings)

    asyncio.run(go())


if __name__ == "__main__":
    main()
