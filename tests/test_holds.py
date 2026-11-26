from datetime import UTC, datetime, timedelta


def hold(client, headers, seat_ids, show_id=1):
    return client.post("/holds", json={"show_id": show_id, "seat_ids": seat_ids}, headers=headers)


def seat_statuses(client, show_id=1):
    return {s["id"]: s["status"] for s in client.get(f"/shows/{show_id}/seats").json()["seats"]}


def test_hold_claims_seats_for_ten_minutes(client, auth):
    response = hold(client, auth(), [1, 2])

    assert response.status_code == 201
    body = response.json()
    assert body["seat_ids"] == [1, 2]
    expires_at = datetime.fromisoformat(body["expires_at"])
    assert timedelta(minutes=9) < expires_at - datetime.now(UTC) <= timedelta(minutes=10)
    statuses = seat_statuses(client)
    assert statuses[1] == statuses[2] == "held"
    assert statuses[3] == "available"


def test_hold_needs_a_token(client):
    assert client.post("/holds", json={"show_id": 1, "seat_ids": [1]}).status_code == 401


def test_hold_rejects_more_than_six_seats(client, auth):
    response = hold(client, auth(), [1, 2, 3, 4, 5, 6, 7])
    assert response.status_code == 422
    assert response.json() == {"detail": "at most 6 seats per hold"}


def test_hold_rejects_duplicate_seats(client, auth):
    assert hold(client, auth(), [1, 1]).status_code == 422


def test_hold_rejects_seats_from_another_show(client, auth):
    response = hold(client, auth(), [1, 51])  # seat 51 belongs to show 2
    assert response.status_code == 422
    assert seat_statuses(client)[1] == "available"  # nothing half-claimed


def test_hold_on_taken_seat_is_409_and_claims_nothing(client, auth):
    hold(client, auth(1), [1])

    response = hold(client, auth(2), [1, 2])

    assert response.status_code == 409
    assert response.json() == {"detail": "seat unavailable"}
    assert seat_statuses(client)[2] == "available"


def test_one_open_hold_per_user_per_show(client, auth):
    hold(client, auth(), [1])

    response = hold(client, auth(), [2])

    assert response.status_code == 409
    assert response.json() == {"detail": "you already have an open hold for this show"}
    assert hold(client, auth(), [51], show_id=2).status_code == 201  # other shows are fine


def test_release_frees_seats_and_allows_a_new_hold(client, auth):
    hold_id = hold(client, auth(), [1, 2]).json()["hold_id"]

    assert client.delete(f"/holds/{hold_id}", headers=auth()).status_code == 204
    assert set(seat_statuses(client).values()) == {"available"}
    assert hold(client, auth(), [3]).status_code == 201


def test_release_twice_is_409(client, auth):
    hold_id = hold(client, auth(), [1]).json()["hold_id"]
    client.delete(f"/holds/{hold_id}", headers=auth())

    response = client.delete(f"/holds/{hold_id}", headers=auth())

    assert response.status_code == 409
    assert response.json() == {"detail": "hold is released"}


def test_cannot_release_someone_elses_hold(client, auth):
    hold_id = hold(client, auth(1), [1]).json()["hold_id"]

    assert client.delete(f"/holds/{hold_id}", headers=auth(2)).status_code == 404
    assert seat_statuses(client)[1] == "held"
