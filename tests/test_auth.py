import jwt

from rushline.auth import ALGORITHM


def test_dev_token_logs_in_seeded_user(client):
    token = client.post("/auth/dev-token", json={"email": "ravi@example.com"}).json()
    me = client.get("/auth/me", headers={"Authorization": f"Bearer {token['access_token']}"})

    assert me.json() == {"user_id": 2, "actor": "user", "run_id": None}


def test_dev_token_unknown_email_is_401(client):
    assert client.post("/auth/dev-token", json={"email": "x@example.com"}).status_code == 401


def test_dev_token_is_hidden_when_disabled(db, monkeypatch):
    from fastapi.testclient import TestClient

    from rushline.main import app

    monkeypatch.setenv("RUSHLINE_ENABLE_DEV_LOGIN", "false")
    with TestClient(app) as client:
        response = client.post("/auth/dev-token", json={"email": "asha@example.com"})
    assert response.status_code == 404


def test_missing_token_is_401_with_bearer_challenge(client):
    response = client.get("/auth/me")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_agent_token_carries_run_id(client, auth):
    assert client.get("/auth/me", headers=auth(3, "agent", run_id=7)).json() == {
        "user_id": 3,
        "actor": "agent",
        "run_id": 7,
    }


def test_rejects_expired_token(client, auth):
    assert client.get("/auth/me", headers=auth(ttl_seconds=-60)).status_code == 401


def test_tolerates_small_clock_skew(client, auth):
    # Expired 5s ago by our clock: within the 30s leeway, e.g. the issuer's clock is behind.
    assert client.get("/auth/me", headers=auth(ttl_seconds=-5)).status_code == 200


def test_rejects_token_signed_with_another_key(client):
    forged = jwt.encode({"sub": "1", "actor": "user", "exp": 9999999999}, "x" * 32, ALGORITHM)
    assert client.get("/auth/me", headers={"Authorization": f"Bearer {forged}"}).status_code == 401


def test_rejects_agent_token_without_run_id(client, auth):
    assert client.get("/auth/me", headers=auth(1, "agent")).status_code == 401


def test_rejects_unknown_actor(client, auth):
    assert client.get("/auth/me", headers=auth(1, "admin")).status_code == 401
