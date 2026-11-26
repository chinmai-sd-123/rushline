from fastapi.testclient import TestClient

from rushline.main import app


def test_health_returns_ok():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_chat_page_is_served():
    response = TestClient(app).get("/chat")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "/runs/" in response.text and "Last-Event-ID" in response.text
