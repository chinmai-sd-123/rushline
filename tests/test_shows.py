import pytest
from fastapi.testclient import TestClient

from rushline.main import app


@pytest.fixture
def client(db):
    with TestClient(app) as c:
        yield c


def test_list_shows_first_page(client):
    body = client.get("/shows").json()

    assert [s["title"] for s in body["items"]] == ["Tatkal Tuesday", "Launch Night"]
    assert body["next_cursor"] is None


def test_list_shows_follows_cursor_to_last_page(client):
    first = client.get("/shows", params={"limit": 1}).json()
    second = client.get("/shows", params={"limit": 1, "cursor": first["next_cursor"]}).json()

    assert [s["title"] for s in first["items"]] == ["Tatkal Tuesday"]
    assert first["next_cursor"] is not None
    assert [s["title"] for s in second["items"]] == ["Launch Night"]
    assert second["next_cursor"] is None


@pytest.mark.parametrize("limit", [0, 101])
def test_list_shows_rejects_bad_limit(client, limit):
    assert client.get("/shows", params={"limit": limit}).status_code == 422


def test_list_shows_rejects_bad_cursor(client):
    response = client.get("/shows", params={"cursor": "garbage"})

    assert response.status_code == 400
    assert response.json() == {"detail": "invalid cursor"}
