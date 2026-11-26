import uuid

import pytest
from fastapi.testclient import TestClient

from rushline import ratelimit
from rushline.main import app


@pytest.fixture
def limited(db, monkeypatch):
    """Holds limited to 2 per minute, with the clock frozen inside one window."""
    monkeypatch.setenv("RUSHLINE_HOLDS_PER_MINUTE", "2")
    monkeypatch.setattr(ratelimit, "_now", lambda: 1_800_000_010.0)
    with TestClient(app) as client:
        yield client


def post(client, headers, seat):
    headers = {**headers, "Idempotency-Key": uuid.uuid4().hex}
    return client.post("/holds", json={"show_id": 2, "seat_ids": [seat]}, headers=headers)


def test_third_request_in_a_minute_is_429(limited, auth):
    codes = [post(limited, auth(), seat).status_code for seat in (51, 52, 53)]

    assert codes[2] == 429
    response = post(limited, auth(), 54)
    assert response.json() == {"detail": "rate limit exceeded"}
    assert response.headers["retry-after"] == "50"  # seconds left in the window


def test_limits_are_per_user(limited, auth):
    post(limited, auth(1), 51)
    post(limited, auth(1), 52)

    assert post(limited, auth(2), 53).status_code != 429


def test_redis_down_falls_back_to_a_stricter_local_limit(db, auth, monkeypatch):
    monkeypatch.setenv("RUSHLINE_HOLDS_PER_MINUTE", "4")
    monkeypatch.setenv("RUSHLINE_REDIS_URL", "redis://localhost:1/0")
    monkeypatch.setattr(ratelimit, "_now", lambda: 1_800_000_010.0)
    monkeypatch.setattr(ratelimit, "_local", ratelimit._LocalWindows())

    with TestClient(app) as client:
        codes = [post(client, auth(), seat).status_code for seat in (51, 52, 53)]

    assert codes[0] != 429  # still serving without Redis...
    assert codes[2] == 429  # ...but at half the limit (2 instead of 4)
