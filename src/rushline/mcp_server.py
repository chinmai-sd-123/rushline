"""MCP server: lets any MCP host browse shows, search policies and hold seats.

A thin layer over the REST API with a per-user `mcp` token. It offers no payment tools,
but that's not what keeps it safe: the API itself refuses mcp tokens for paying,
approving and cancelling (ADR 004). Payment happens in the chat page or the API.

    uv run python -m rushline.auth mcp-token asha@example.com   # once
    RUSHLINE_MCP_TOKEN=... uv run python -m rushline.mcp_server  # stdio, for MCP hosts
"""

import uuid

import httpx2
from mcp.server.mcpserver import MCPServer

from rushline.agent import tools
from rushline.config import Settings

# Test seam: route API calls to the in-process app instead of the network.
api_transport: httpx2.AsyncBaseTransport | None = None

server = MCPServer(
    name="rushline",
    instructions=(
        "Browse Rushline shows and seat maps, search venue policies (cite them), and hold "
        "seats for 10 minutes. Paying needs the Rushline app: holds here are free and expire."
    ),
)


def _api() -> tools.ApiClient:
    settings = Settings()
    if settings.mcp_token is None:
        raise RuntimeError("set RUSHLINE_MCP_TOKEN (python -m rushline.auth mcp-token <email>)")
    return tools.ApiClient(
        settings.api_url,
        settings.mcp_token.get_secret_value(),
        key_prefix=f"mcp-{uuid.uuid4().hex}",
        transport=api_transport,
    )


async def _run(name: str, args: dict) -> dict:
    api = _api()
    try:
        return await tools.execute(api, name, args, call_id=uuid.uuid4().hex)
    finally:
        await api.aclose()


@server.tool(description="List upcoming shows with ids and start times.")
async def list_shows() -> dict:
    return await _run("list_shows", {})


@server.tool(description="Available seats for a show, by row, with prices in rupees.")
async def seat_map(show_id: int) -> dict:
    return await _run("seat_map", {"show_id": show_id})


@server.tool(description="Search venue policies. Results include a citation to quote.")
async def search_policies(question: str, venue_id: int | None = None) -> dict:
    return await _run("search_policies", {"question": question, "venue_id": venue_id})


@server.tool(description="Hold up to 6 seats in one show for 10 minutes. Free; expires.")
async def hold_seats(show_id: int, seat_ids: list[int]) -> dict:
    return await _run("hold_seats", {"show_id": show_id, "seat_ids": seat_ids})


@server.tool(description="Release a hold so its seats go back on sale.")
async def release_hold(hold_id: int) -> dict:
    return await _run("release_hold", {"hold_id": hold_id})


if __name__ == "__main__":
    server.run("stdio")
