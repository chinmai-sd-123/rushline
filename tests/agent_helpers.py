"""Shared pieces for agent tests: the in-process app, scripted models and run helpers."""

import json

import httpx2
import pytest
from helpers import aquery
from redis.asyncio import Redis

from rushline.agent import worker
from rushline.agent.gateway import LLMReply, ToolCall
from rushline.auth import create_token
from rushline.config import Settings
from rushline.main import app
from rushline.queue import Job


@pytest.fixture
async def env(db, anyio_backend):
    """The app running in-process, with the agent's API calls routed to it."""
    settings = Settings()
    redis = Redis.from_url(settings.redis_url)
    async with app.router.lifespan_context(app):
        worker.api_transport = httpx2.ASGITransport(app=app)
        client = httpx2.AsyncClient(transport=worker.api_transport, base_url="http://test")
        yield settings, redis, client
        await client.aclose()
        worker.api_transport = None
    await redis.aclose()


def user(settings, user_id=1):
    return {"Authorization": f"Bearer {create_token(settings, user_id)}"}


async def start_run(client, settings, text="Book 2 seats for Tatkal Tuesday", user_id=1):
    headers = user(settings, user_id)
    conversation = (await client.post("/conversations", headers=headers)).json()
    response = await client.post(
        f"/conversations/{conversation['conversation_id']}/messages",
        json={"text": text},
        headers=headers,
    )
    return response.json()["run_id"], conversation["conversation_id"]


async def last_outbox_job() -> Job:
    [(event_type, payload)] = await aquery(
        "SELECT event_type, payload FROM outbox "
        "WHERE event_type LIKE 'run.%' ORDER BY id DESC LIMIT 1"
    )
    return Job("0-1", event_type, payload)


async def run_status(client, settings, run_id, user_id=1):
    return (await client.get(f"/runs/{run_id}", headers=user(settings, user_id))).json()


async def events(redis, run_id):
    entries = await redis.xrange(worker.run_events_key(run_id))
    return [fields[b"type"].decode() for _, fields in entries]


def tool_calls_so_far(messages):
    return [
        c["function"]["name"]
        for m in messages
        if m["role"] == "assistant"
        for c in m.get("tool_calls", [])
    ]


def result_of(messages, tool):
    """Body of the last result of `tool`, found by matching tool_call ids."""
    ids = {
        c["id"]
        for m in messages
        if m["role"] == "assistant"
        for c in m.get("tool_calls", [])
        if c["function"]["name"] == tool
    }
    for m in reversed(messages):
        if m["role"] == "tool" and m["tool_call_id"] in ids:
            return json.loads(m["content"])["body"]
    return None


def call(messages, name, **args) -> LLMReply:
    n = len(tool_calls_so_far(messages))
    return LLMReply(None, [ToolCall(f"call_{n}", name, args)], tokens_in=100, tokens_out=10)


def booking_model(messages):
    """Plays a well-behaved LLM: picks the next step from the conversation."""
    done = tool_calls_so_far(messages)
    if "seat_map" not in done:
        return call(messages, "seat_map", show_id=1)
    if "hold_seats" not in done:
        return call(messages, "hold_seats", show_id=1, seat_ids=[1, 2])
    hold = result_of(messages, "hold_seats")
    if "request_payment_approval" not in done:
        return call(messages, "request_payment_approval", hold_id=hold["hold_id"])
    decision = messages[-1]["content"] if messages[-1]["role"] == "system" else ""
    if "approved" in decision and "pay_for_hold" not in done:
        approval = result_of(messages, "request_payment_approval")
        return call(
            messages, "pay_for_hold", hold_id=hold["hold_id"], approval_id=approval["approval_id"]
        )
    if "rejected" in decision and "release_hold" not in done:
        return call(messages, "release_hold", hold_id=hold["hold_id"])
    paid = result_of(messages, "pay_for_hold")
    text = f"Booked: booking {paid['booking_id']}." if paid else "OK, I released the seats."
    return LLMReply(text, tokens_in=50, tokens_out=20)
