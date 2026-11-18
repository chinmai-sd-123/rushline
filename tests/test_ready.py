from fastapi.testclient import TestClient

from rushline.main import app

DEAD_POSTGRES = "postgresql+asyncpg://rushline:rushline@localhost:1/rushline"
DEAD_REDIS = "redis://localhost:1/0"


def test_ready_ok_when_everything_is_up():
    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"postgres": "ok", "redis": "ok"}}


def test_ready_degraded_when_redis_is_down(monkeypatch):
    monkeypatch.setenv("RUSHLINE_REDIS_URL", DEAD_REDIS)

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"
    assert response.json()["checks"]["redis"] == "down"


def test_ready_unavailable_when_postgres_is_down(monkeypatch):
    monkeypatch.setenv("RUSHLINE_DATABASE_URL", DEAD_POSTGRES)

    with TestClient(app) as client:
        response = client.get("/ready")
        health = client.get("/health")

    assert response.status_code == 503
    assert response.json()["checks"]["postgres"] == "down"
    assert health.status_code == 200
