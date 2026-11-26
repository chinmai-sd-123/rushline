import httpx2
import pytest
from fastapi.testclient import TestClient

from rushline.main import app


@pytest.mark.anyio
async def test_exhausted_pool_returns_retryable_503(db, monkeypatch):
    monkeypatch.setenv("RUSHLINE_DB_POOL_SIZE", "1")
    monkeypatch.setenv("RUSHLINE_DB_MAX_OVERFLOW", "0")
    monkeypatch.setenv("RUSHLINE_DB_POOL_TIMEOUT_SECONDS", "0.1")
    async with app.router.lifespan_context(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://test") as client:
            async with app.state.engine.connect():  # take the only connection
                response = await client.get("/shows")

    assert response.status_code == 503
    assert response.json() == {"detail": "server busy, try again"}
    assert response.headers["retry-after"] == "1"


def test_database_down_returns_503(db, monkeypatch):
    monkeypatch.setenv("RUSHLINE_DATABASE_URL", "postgresql+asyncpg://u:p@localhost:1/x")
    with TestClient(app) as client:
        response = client.get("/shows")

    assert response.status_code == 503
    assert response.json() == {"detail": "database unavailable"}
