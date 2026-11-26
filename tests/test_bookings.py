from helpers import book, hold, query

from rushline import mockpay


def statuses(seat_ids):
    rows = query("SELECT status FROM seats WHERE id = ANY(:ids) ORDER BY id", ids=seat_ids)
    return [r.status for r in rows]


def hold_status(hold_id):
    return query("SELECT status FROM holds WHERE id = :id", id=hold_id)[0].status


def test_booking_authorizes_captures_and_confirms(client, auth):
    hold_id = hold(client, auth(), [1, 2])

    response = book(client, auth(), hold_id)

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "confirmed"
    assert body["amount_paise"] == 100000  # two row-A seats at 50000
    assert statuses([1, 2]) == ["booked", "booked"]
    assert hold_status(hold_id) == "converted"
    [payment] = mockpay.payments.values()
    assert payment.status == "captured" and payment.key == f"hold-{hold_id}"
    events = [r.event_type for r in query("SELECT event_type FROM outbox ORDER BY id")]
    assert events == ["booking.created", "booking.confirmed"]
    assert client.get("/bookings", headers=auth()).json()[0]["status"] == "confirmed"


def test_declined_payment_releases_the_hold(client, auth):
    hold_id = hold(client, auth(), [1, 2])

    response = book(client, auth(), hold_id, token=mockpay.DECLINE_TOKEN)

    assert response.status_code == 402
    assert hold_status(hold_id) == "released"
    assert statuses([1, 2]) == ["available", "available"]
    assert query("SELECT count(*) FROM bookings")[0][0] == 0


def test_expired_hold_is_410_and_nothing_is_charged(client, auth):
    hold_id = hold(client, auth(), [1])
    query("UPDATE holds SET expires_at = now() - interval '1 second' WHERE id = :id", id=hold_id)

    response = book(client, auth(), hold_id)

    assert response.status_code == 410
    assert mockpay.payments == {}


def test_authorize_timeout_leaves_the_hold_paying_for_reconciliation(client, auth):
    hold_id = hold(client, auth(), [1, 2])
    mockpay.queued["authorize"].append("timeout_after")  # charged, but we never hear back

    response = book(client, auth(), hold_id)

    assert response.status_code == 202
    assert response.json() == {
        "booking_id": None,
        "hold_id": hold_id,
        "amount_paise": 100000,
        "status": "processing",
    }
    assert hold_status(hold_id) == "paying"
    assert client.get(f"/holds/{hold_id}", headers=auth()).json()["status"] == "paying"


def test_capture_failure_keeps_the_booking_pending_capture(client, auth):
    hold_id = hold(client, auth(), [1])
    mockpay.queued["capture"].append("fail")

    response = book(client, auth(), hold_id)

    assert response.status_code == 201
    assert response.json()["status"] == "pending_capture"
    assert query("SELECT status FROM bookings")[0].status == "pending_capture"


def test_retry_with_same_key_replays_and_books_once(client, auth):
    hold_id = hold(client, auth(), [1])

    first = book(client, auth(), hold_id, key="pay-1")
    again = book(client, auth(), hold_id, key="pay-1")

    assert again.json() == first.json()
    assert again.headers["idempotent-replayed"] == "true"
    assert query("SELECT count(*) FROM bookings")[0][0] == 1
    assert len(mockpay.payments) == 1


def test_only_users_can_pay_for_now(client, auth):
    hold_id = hold(client, auth(), [1])

    assert book(client, auth(1, "agent", run_id=1), hold_id).status_code == 403
    assert book(client, auth(1, "mcp"), hold_id).status_code == 403


def test_cannot_pay_for_someone_elses_hold(client, auth):
    hold_id = hold(client, auth(1), [1])
    assert book(client, auth(2), hold_id).status_code == 404


def test_paying_twice_for_one_hold_is_409(client, auth):
    hold_id = hold(client, auth(), [1])
    book(client, auth(), hold_id)

    response = book(client, auth(), hold_id)

    assert response.status_code == 409
    assert response.json() == {"detail": "hold is converted"}


def test_cancel_confirmed_booking_frees_the_seats(client, auth):
    booking_id = book(client, auth(), hold(client, auth(), [1, 2])).json()["booking_id"]

    assert client.delete(f"/bookings/{booking_id}", headers=auth()).status_code == 204
    assert statuses([1, 2]) == ["available", "available"]
    assert client.delete(f"/bookings/{booking_id}", headers=auth()).status_code == 409
    assert client.delete(f"/bookings/{booking_id}", headers=auth(2)).status_code == 404


def test_cannot_cancel_while_capture_is_pending(client, auth):
    mockpay.queued["capture"].append("fail")
    booking_id = book(client, auth(), hold(client, auth(), [1])).json()["booking_id"]

    response = client.delete(f"/bookings/{booking_id}", headers=auth())

    assert response.status_code == 409
    assert response.json() == {"detail": "booking is pending_capture"}
