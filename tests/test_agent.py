"""The agent end to end: a scripted model, the real graph, the real API and database."""

import json
import os

import httpx2
import pytest
from helpers import aquery
from langgraph.checkpoint.memory import InMemorySaver
from redis.asyncio import Redis

from rushline.agent import worker
from rushline.agent.gateway import FakeProvider, Gateway, LLMReply, ToolCall
from rushline.agent.loop import run_simple
from rushline.agent.tools import ApiClient
from rushline.auth import create_token
from rushline.config import Settings
from rushline.main import app
from rushline.queue import Job, JobQueue


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


def booking_model(messages):
    """Plays the LLM: picks the next step from the conversation, like a model would."""
    done = tool_calls_so_far(messages)

    def call(name, **args):
        return LLMReply(
            None, [ToolCall(f"call_{len(done)}", name, args)], tokens_in=100, tokens_out=10
        )

    if "seat_map" not in done:
        return call("seat_map", show_id=1)
    if "hold_seats" not in done:
        return call("hold_seats", show_id=1, seat_ids=[1, 2])
    hold = result_of(messages, "hold_seats")
    if "request_payment_approval" not in done:
        return call("request_payment_approval", hold_id=hold["hold_id"])
    decision = messages[-1]["content"] if messages[-1]["role"] == "system" else ""
    if "approved" in decision and "pay_for_hold" not in done:
        approval = result_of(messages, "request_payment_approval")
        return call("pay_for_hold", hold_id=hold["hold_id"], approval_id=approval["approval_id"])
    if "rejected" in decision and "release_hold" not in done:
        return call("release_hold", hold_id=hold["hold_id"])
    paid = result_of(messages, "pay_for_hold")
    text = f"Booked: booking {paid['booking_id']}." if paid else "OK, I released the seats."
    return LLMReply(text, tokens_in=50, tokens_out=20)


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


async def start_run(client, settings, text="Book 2 seats for Tatkal Tuesday"):
    conversation = (await client.post("/conversations", headers=user(settings))).json()
    response = await client.post(
        f"/conversations/{conversation['conversation_id']}/messages",
        json={"text": text},
        headers=user(settings),
    )
    return response.json()["run_id"], conversation["conversation_id"]


async def last_outbox_job() -> Job:
    [(event_type, payload)] = await aquery(
        "SELECT event_type, payload FROM outbox "
        "WHERE event_type LIKE 'run.%' ORDER BY id DESC LIMIT 1"
    )
    return Job("0-1", event_type, payload)


async def run_status(client, settings, run_id):
    return (await client.get(f"/runs/{run_id}", headers=user(settings))).json()


async def events(redis, run_id):
    entries = await redis.xrange(worker.run_events_key(run_id))
    return [fields[b"type"].decode() for _, fields in entries]


@pytest.mark.anyio
async def test_agent_holds_pauses_for_approval_then_books(env):
    settings, redis, client = env
    gateway = Gateway(FakeProvider(booking_model), ["fake"], redis, 100_000, 5)
    saver = InMemorySaver()
    run_id, _ = await start_run(client, settings)

    await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)

    paused = await run_status(client, settings, run_id)
    assert paused["status"] == "waiting_approval"
    [approval] = (await client.get(f"/runs/{run_id}/approvals", headers=user(settings))).json()
    assert approval["status"] == "pending" and approval["amount_paise"] == 100000
    assert (await events(redis, run_id))[-1] == "approval_required"

    decided = await client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve", "payment_token": "tok_ok"},
        headers=user(settings),
    )
    assert decided.status_code == 200
    await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)

    done = await run_status(client, settings, run_id)
    assert done["status"] == "completed"
    assert done["output"].startswith("Booked: booking")
    assert done["tokens_in"] == 450 and done["tokens_out"] == 60  # both legs added up
    [(status,)] = await aquery("SELECT status FROM bookings")
    assert status == "confirmed"
    assert (await events(redis, run_id))[-1] == "run.completed"


@pytest.mark.anyio
async def test_rejected_approval_releases_the_hold_and_charges_nothing(env):
    settings, redis, client = env
    gateway = Gateway(FakeProvider(booking_model), ["fake"], redis, 100_000, 5)
    saver = InMemorySaver()
    run_id, _ = await start_run(client, settings)
    await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)
    [approval] = (await client.get(f"/runs/{run_id}/approvals", headers=user(settings))).json()

    await client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "reject"},
        headers=user(settings),
    )
    await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)

    assert (await run_status(client, settings, run_id))["output"] == "OK, I released the seats."
    assert await aquery("SELECT count(*) FROM bookings") == [(0,)]
    assert await aquery("SELECT status FROM holds") == [("released",)]


@pytest.mark.anyio
async def test_stream_replays_and_resumes_from_last_event_id(env):
    settings, redis, client = env
    gateway = Gateway(FakeProvider(booking_model), ["fake"], redis, 100_000, 5)
    saver = InMemorySaver()
    run_id, _ = await start_run(client, settings)
    await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)

    first = await client.get(f"/runs/{run_id}/stream", headers=user(settings))
    blocks = [b for b in first.text.split("\n\n") if b.startswith("id:")]
    last_id = blocks[-1].split("\n")[0].removeprefix("id: ")

    assert first.headers["content-type"].startswith("text/event-stream")
    assert "event: approval_required" in blocks[-1]  # the stream ends where the run paused

    [approval] = (await client.get(f"/runs/{run_id}/approvals", headers=user(settings))).json()
    await client.post(
        f"/runs/{run_id}/approvals/{approval['approval_id']}",
        json={"decision": "approve", "payment_token": "tok_ok"},
        headers=user(settings),
    )
    await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)
    rest = await client.get(
        f"/runs/{run_id}/stream", headers={**user(settings), "Last-Event-ID": last_id}
    )

    assert "event: run.started" in rest.text and "event: approval_required" not in rest.text
    assert rest.text.rstrip().split("\n")[-2] == "event: run.completed"


@pytest.mark.anyio
async def test_stream_is_private(env):
    settings, _, client = env
    run_id, _ = await start_run(client, settings)

    assert (
        await client.get(f"/runs/{run_id}/stream", headers=user(settings, 2))
    ).status_code == 404


@pytest.mark.anyio
async def test_plain_loop_stops_at_the_approval_request(env):
    settings, redis, client = env
    run_id, _ = await start_run(client, settings)
    token = create_token(settings, 1, "agent", run_id=run_id)
    api = ApiClient("http://test", token, f"run{run_id}", transport=worker.api_transport)
    gateway = Gateway(FakeProvider(booking_model), ["fake"], redis, 100_000, 5)

    answer, messages = await run_simple(gateway, api, 1, "book 2 seats")
    await api.aclose()

    assert answer == "Waiting for your approval to pay."
    assert tool_calls_so_far(messages) == ["seat_map", "hold_seats", "request_payment_approval"]


@pytest.mark.anyio
async def test_queue_redelivers_a_job_whose_worker_died(env):
    settings, redis, _ = env
    await redis.delete("rushline:agent-jobs")
    queue = JobQueue(redis, visibility_seconds=0)  # everything counts as stale at once
    await queue.ensure_group()
    await queue.put(1, "run.queued", {"run_id": 1})

    taken = await queue.take("worker-a", block_ms=100)  # worker-a dies without acking
    stolen = await queue.take("worker-b", block_ms=100)
    await queue.ack(stolen)

    assert taken.payload == stolen.payload == {"run_id": 1}
    assert await queue.take("worker-c", block_ms=100) is None


@pytest.mark.anyio
async def test_checkpoints_live_in_the_agent_schema(env):
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    settings, redis, client = env
    run_id, _ = await start_run(client, settings)
    gateway = Gateway(FakeProvider(booking_model), ["fake"], redis, 100_000, 5)
    url = worker.checkpoint_url(Settings(database_url=os.environ["RUSHLINE_DATABASE_URL"]))

    async with AsyncPostgresSaver.from_conn_string(url) as saver:
        await saver.setup()
        await worker.handle(await last_outbox_job(), settings, gateway, redis, saver)

    tables = {
        r[0]
        for r in await aquery(
            "SELECT table_schema FROM information_schema.tables WHERE table_name LIKE 'checkpoint%'"
        )
    }
    assert tables == {"agent"}
    assert (await run_status(client, settings, run_id))["status"] == "waiting_approval"
