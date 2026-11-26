import asyncio
import json

import pytest
from helpers import aquery, book, hold, query
from redis.asyncio import Redis

from rushline import mockpay, worker
from rushline.config import Settings
from rushline.db import create_engine
from rushline.outbox import STREAM, RedisStreamPublisher
from rushline.payments import PaymentClient


@pytest.fixture
def settings():
    return Settings()


def run(job, settings, *args):
    """Run one worker job with its own engine and payment client."""

    async def go():
        engine = create_engine(settings)
        payments = PaymentClient(settings.payment_url, settings.payment_timeout_seconds)
        try:
            return await job(engine, payments, *args)
        finally:
            await payments.aclose()
            await engine.dispose()

    return asyncio.run(go())


def expire(settings):
    return run(lambda engine, _p: worker.expire_holds(engine, 50), settings)


def reconcile(settings):
    return run(lambda engine, p: worker.reconcile_payments(engine, p, 50), settings)


def captures(settings):
    # Backdate first: WSL's clock can step backwards, which would make "due" flaky.
    query("UPDATE bookings SET created_at = now() - interval '1 minute'")
    return run(lambda engine, p: worker.retry_captures(engine, p, settings), settings)


def past_deadline(hold_id):
    query(
        "UPDATE holds SET expires_at = now() - interval '1 minute', "
        "payment_deadline = now() - interval '1 second' WHERE id = :id",
        id=hold_id,
    )


def one(sql, **params):
    return query(sql, **params)[0][0]


# --- Hold expiry ---


def test_expired_active_holds_free_their_seats(client, auth, settings):
    hold_id = hold(client, auth(), [1, 2])
    past_deadline(hold_id)

    assert expire(settings) == 1
    assert one("SELECT status FROM holds WHERE id = :id", id=hold_id) == "expired"
    assert one("SELECT count(*) FROM seats WHERE status = 'available' AND id IN (1, 2)") == 2


def test_expiry_never_touches_a_paying_hold(client, auth, settings):
    hold_id = hold(client, auth(), [1])
    mockpay.queued["authorize"].append("timeout_after")
    book(client, auth(), hold_id)  # 202: paying
    past_deadline(hold_id)

    assert expire(settings) == 0
    assert one("SELECT status FROM holds WHERE id = :id", id=hold_id) == "paying"


def test_two_workers_never_expire_the_same_hold_twice(client, auth, settings):
    for user, seat in [(1, 1), (2, 2), (3, 3)]:
        past_deadline(hold(client, auth(user), [seat]))

    async def both():
        engine = create_engine(settings)
        try:
            return await asyncio.gather(
                worker.expire_holds(engine, 50), worker.expire_holds(engine, 50)
            )
        finally:
            await engine.dispose()

    assert sum(asyncio.run(both())) == 3


# --- Payment reconciliation (ADR 003) ---


def test_reconcile_books_when_the_timed_out_authorize_actually_happened(client, auth, settings):
    hold_id = hold(client, auth(), [1, 2])
    mockpay.queued["authorize"].append("timeout_after")
    assert book(client, auth(), hold_id).status_code == 202
    past_deadline(hold_id)

    assert reconcile(settings) == 1
    assert one("SELECT status FROM holds WHERE id = :id", id=hold_id) == "converted"
    assert one("SELECT status FROM bookings WHERE hold_id = :id", id=hold_id) == "pending_capture"

    assert captures(settings) == 1
    assert one("SELECT status FROM bookings WHERE hold_id = :id", id=hold_id) == "confirmed"
    [payment] = mockpay.payments.values()
    assert payment.status == "captured"  # charged exactly once, for a real booking


def test_reconcile_releases_when_the_provider_never_saw_the_payment(client, auth, settings):
    hold_id = hold(client, auth(), [1, 2])
    mockpay.queued["authorize"].append("timeout_before")
    assert book(client, auth(), hold_id).status_code == 202
    past_deadline(hold_id)

    assert reconcile(settings) == 1
    assert one("SELECT status FROM holds WHERE id = :id", id=hold_id) == "released"
    assert one("SELECT count(*) FROM seats WHERE status = 'available' AND id IN (1, 2)") == 2
    assert mockpay.payments == {}  # nothing charged


def test_reconcile_waits_while_the_provider_is_unreachable(client, auth, settings):
    hold_id = hold(client, auth(), [1])
    mockpay.queued["authorize"].append("timeout_after")
    book(client, auth(), hold_id)
    past_deadline(hold_id)
    mockpay.queued["lookup"].append("timeout_before")

    assert reconcile(settings) == 0
    assert one("SELECT status FROM holds WHERE id = :id", id=hold_id) == "paying"


def test_reconcile_ignores_paying_holds_before_their_deadline(client, auth, settings):
    hold_id = hold(client, auth(), [1])
    mockpay.queued["authorize"].append("timeout_after")
    book(client, auth(), hold_id)

    assert reconcile(settings) == 0


# --- Capture retries ---


def test_crash_between_saving_and_capturing_ends_confirmed(client, auth, settings):
    """The scenario the pending_capture state exists for (ADR 003)."""
    mockpay.queued["capture"].append("timeout_before")  # the API never gets to capture
    booking_id = book(client, auth(), hold(client, auth(), [1])).json()["booking_id"]

    assert captures(settings) == 1
    assert one("SELECT status FROM bookings WHERE id = :id", id=booking_id) == "confirmed"


def test_expired_authorization_marks_capture_failed(client, auth, settings):
    mockpay.queued["capture"].extend(["fail", "expired"])
    booking_id = book(client, auth(), hold(client, auth(), [1])).json()["booking_id"]

    assert captures(settings) == 0
    assert one("SELECT status FROM bookings WHERE id = :id", id=booking_id) == "capture_failed"


def test_capture_gives_up_after_max_attempts(client, auth, settings, monkeypatch):
    monkeypatch.setenv("RUSHLINE_CAPTURE_MAX_ATTEMPTS", "2")
    settings = Settings()
    mockpay.queued["capture"].extend(["fail"] * 3)  # API's own try, then two retries
    booking_id = book(client, auth(), hold(client, auth(), [1])).json()["booking_id"]

    captures(settings)
    assert one("SELECT status FROM bookings WHERE id = :id", id=booking_id) == "pending_capture"
    captures(settings)
    assert one("SELECT status FROM bookings WHERE id = :id", id=booking_id) == "capture_failed"


# --- Outbox ---


def test_outbox_events_are_published_once(client, auth, settings):
    book(client, auth(), hold(client, auth(), [1]))

    async def publish_twice():
        engine = create_engine(settings)
        redis = Redis.from_url(settings.redis_url)
        publisher = RedisStreamPublisher(redis)
        try:
            first = await worker.publish_outbox(engine, publisher, 50)
            second = await worker.publish_outbox(engine, publisher, 50)
            entries = await redis.xrange(STREAM)
            return first, second, entries
        finally:
            await redis.aclose()
            await engine.dispose()

    first, second, entries = asyncio.run(publish_twice())

    assert (first, second) == (2, 0)
    assert [e[1][b"type"] for e in entries] == [b"booking.created", b"booking.confirmed"]
    assert json.loads(entries[0][1][b"payload"])["hold_id"] == 1
    assert one("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0


# --- Graceful shutdown ---


@pytest.mark.anyio
async def test_worker_stops_cleanly_when_asked(db, settings):
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop, settings))
    await asyncio.sleep(0.3)  # let it start ticking

    stop.set()
    await asyncio.wait_for(task, timeout=5)  # finishes the tick, closes clients, exits

    assert task.done() and task.exception() is None
    assert (await aquery("SELECT 1"))[0][0] == 1
