"""The MCP server can browse and hold, and the API refuses payment for its token."""

import json

import httpx2
import pytest

from rushline import mcp_server
from rushline.auth import create_token
from rushline.config import Settings
from rushline.main import app


@pytest.fixture
async def mcp(db, anyio_backend, monkeypatch):
    settings = Settings()
    monkeypatch.setenv("RUSHLINE_MCP_TOKEN", create_token(settings, 1, "mcp"))
    async with app.router.lifespan_context(app):
        mcp_server.api_transport = httpx2.ASGITransport(app=app)
        yield mcp_server.server
        mcp_server.api_transport = None


def payload(result) -> dict:
    """call_tool returns a CallToolResult whose first content block is our dict as JSON."""
    assert not result.is_error
    return json.loads(result.content[0].text)


@pytest.mark.anyio
async def test_lists_only_browse_and_hold_tools(mcp):
    names = {t.name for t in await mcp.list_tools()}

    assert names == {"list_shows", "seat_map", "search_policies", "hold_seats", "release_hold"}


@pytest.mark.anyio
async def test_holds_seats_for_the_tokens_user(mcp):
    result = payload(await mcp.call_tool("hold_seats", {"show_id": 1, "seat_ids": [5, 6]}))

    assert result["status"] == 201
    assert result["body"]["seat_ids"] == [5, 6]


@pytest.mark.anyio
async def test_the_api_refuses_payment_with_an_mcp_token(mcp):
    hold = payload(await mcp.call_tool("hold_seats", {"show_id": 1, "seat_ids": [7]}))
    token = Settings().mcp_token.get_secret_value()

    async with httpx2.AsyncClient(transport=mcp_server.api_transport, base_url="http://t") as c:
        pay = await c.post(
            "/bookings",
            json={"hold_id": hold["body"]["hold_id"], "payment_token": "tok_ok"},
            headers={"Authorization": f"Bearer {token}", "Idempotency-Key": "k"},
        )

    assert pay.status_code == 403
