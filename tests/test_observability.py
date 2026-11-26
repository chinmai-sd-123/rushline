import json
import logging

from rushline.observability import JsonFormatter


def test_every_response_gets_a_request_id(client):
    first = client.get("/health").headers["x-request-id"]
    second = client.get("/health").headers["x-request-id"]

    assert len(first) == 32 and first != second


def test_safe_caller_request_id_is_kept(client):
    response = client.get("/health", headers={"X-Request-ID": "trace-abc.123"})
    assert response.headers["x-request-id"] == "trace-abc.123"


def test_unsafe_caller_request_id_is_replaced(client):
    response = client.get("/health", headers={"X-Request-ID": "x" * 65})
    assert response.headers["x-request-id"] != "x" * 65


def test_access_log_is_one_json_line_with_the_request_id(client, caplog):
    with caplog.at_level(logging.INFO, logger="rushline.access"):
        client.get("/shows/999/seats", headers={"X-Request-ID": "req-1"})

    [record] = [r for r in caplog.records if r.name == "rushline.access"]
    line = json.loads(JsonFormatter().format(record))
    assert line["msg"] == "request"
    assert line["request_id"] == "req-1"
    assert {k: line[k] for k in ("method", "path", "status")} == {
        "method": "GET",
        "path": "/shows/999/seats",
        "status": 404,
    }
    assert line["duration_ms"] >= 0
