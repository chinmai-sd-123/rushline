"""Agent worker: takes run jobs off the queue and runs (or resumes) the agent graph.

It reaches booking data only through the REST API, with a short-lived token it mints for
the one run it's working on (so no bearer token ever sits in the queue). Its only database
access is LangGraph's checkpoint tables in the `agent` schema.

Run: uv run python -m rushline.agent.worker   (stops after the current job on SIGTERM)
"""

import asyncio
import json
import logging
import signal
import socket
from typing import Any

import httpx2
from langgraph.types import Command
from redis.asyncio import Redis

from rushline.agent.gateway import FakeProvider, Gateway, LLMReply, OpenAIProvider
from rushline.agent.graph import build_graph
from rushline.agent.tools import ApiClient
from rushline.auth import create_token
from rushline.config import Settings
from rushline.observability import configure_logging
from rushline.queue import Job, JobQueue

log = logging.getLogger("rushline.agent")

# Test seam: route API calls to the in-process app instead of the network.
api_transport: httpx2.AsyncBaseTransport | None = None

EVENTS_MAX_LEN = 500
EVENTS_TTL_SECONDS = 24 * 3600
AGENT_TOKEN_TTL_SECONDS = 900


def run_events_key(run_id: int) -> str:
    return f"run:{run_id}:events"


async def emit(redis: Redis, run_id: int, event_type: str, data: dict) -> None:
    """Append a live event for the run; GET /runs/{id}/stream replays and follows these."""
    key = run_events_key(run_id)
    await redis.xadd(
        key, {"type": event_type, "data": json.dumps(data, default=str)}, maxlen=EVENTS_MAX_LEN
    )
    await redis.expire(key, EVENTS_TTL_SECONDS)


def make_gateway(settings: Settings, redis: Redis | None) -> Gateway:
    if settings.openai_api_key is not None:
        provider = OpenAIProvider(
            settings.openai_api_key.get_secret_value(), settings.llm_timeout_seconds
        )
        models = settings.llm_models
    else:  # offline: explain what's missing instead of failing mysteriously
        provider = FakeProvider(
            lambda _: LLMReply("The agent needs RUSHLINE_OPENAI_API_KEY to answer.")
        )
        models = ["fake"]
    return Gateway(
        provider, models, redis, settings.llm_daily_tokens_per_user, settings.llm_timeout_seconds
    )


async def handle(
    job: Job, settings: Settings, gateway: Gateway, redis: Redis, checkpointer
) -> None:
    run_id, user_id = job.payload["run_id"], job.payload.get("user_id")
    token = create_token(
        settings, user_id, "agent", run_id=run_id, ttl_seconds=AGENT_TOKEN_TTL_SECONDS
    )
    api = ApiClient(settings.api_url, token, key_prefix=f"run{run_id}", transport=api_transport)
    try:
        run = (await api.call("GET", f"/runs/{run_id}"))["body"]
        graph = build_graph(
            gateway,
            api,
            user_id,
            run_id,
            settings.run_max_steps,
            settings.conversation_token_budget,
            checkpointer,
        )
        config = {"configurable": {"thread_id": f"conversation-{run['conversation_id']}"}}
        before = (await graph.aget_state(config)).values
        if job.type == "run.queued":
            graph_input: Any = {"messages": [{"role": "user", "content": run["input"]}], "steps": 0}
        else:  # run.resume: carry on from the interrupt with the user's decision
            graph_input = Command(resume=job.payload["decision"])

        await api.call("PATCH", f"/runs/{run_id}", json_body={"status": "running"})
        await emit(redis, run_id, "run.started", {"run_id": run_id})
        async for update in graph.astream(graph_input, config, stream_mode="updates"):
            for node, data in update.items():
                await _emit_update(redis, run_id, node, data)

        state = await graph.aget_state(config)
        usage = {  # this run's share: the state holds the whole conversation's usage
            "tokens_in": state.values.get("tokens_in", 0) - before.get("tokens_in", 0),
            "tokens_out": state.values.get("tokens_out", 0) - before.get("tokens_out", 0),
            "cost_usd": round(state.values.get("cost_usd", 0.0) - before.get("cost_usd", 0.0), 6),
        }
        if state.next:  # paused at wait_for_user
            approval = state.values["pending_approval"]
            await api.call(
                "PATCH", f"/runs/{run_id}", json_body={"status": "waiting_approval", **usage}
            )
            await emit(redis, run_id, "approval_required", approval)
            return
        answer = state.values["messages"][-1].get("content") or ""
        await api.call(
            "PATCH",
            f"/runs/{run_id}",
            json_body={"status": "completed", "output": answer, **usage},
        )
        await emit(redis, run_id, "run.completed", {"output": answer})
    except Exception as exc:
        log.exception("run failed", extra={"fields": {"run_id": run_id}})
        message = f"The agent couldn't finish: {type(exc).__name__}."
        await api.call(
            "PATCH", f"/runs/{run_id}", json_body={"status": "failed", "output": message}
        )
        await emit(redis, run_id, "run.failed", {"output": message})
    finally:
        await api.aclose()


async def _emit_update(redis: Redis, run_id: int, node: str, data: Any) -> None:
    if node == "model" and data:
        message = data["messages"][-1]
        for call in message.get("tool_calls", []):
            await emit(redis, run_id, "tool_call", {"name": call["function"]["name"]})
        if message.get("content"):
            await emit(redis, run_id, "message", {"content": message["content"]})
    elif node == "tools" and data:
        for result in data["messages"]:
            await emit(
                redis, run_id, "tool_result", {"status": json.loads(result["content"])["status"]}
            )


def checkpoint_url(settings: Settings) -> str:
    """psycopg URL with search_path=agent, so LangGraph's tables live in the agent schema."""
    url = settings.agent_database_url or settings.database_url
    return url.replace("+asyncpg", "") + "?options=-csearch_path%3Dagent"


async def run(stop: asyncio.Event, settings: Settings | None = None) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    settings = settings or Settings()
    redis = Redis.from_url(settings.redis_url, socket_timeout=10)
    queue = JobQueue(redis, settings.agent_job_visibility_seconds)
    await queue.ensure_group()
    gateway = make_gateway(settings, redis)
    consumer = f"{socket.gethostname()}-{id(stop)}"
    async with AsyncPostgresSaver.from_conn_string(checkpoint_url(settings)) as checkpointer:
        await checkpointer.setup()
        log.info("agent worker started")
        while not stop.is_set():
            job = await queue.take(consumer, block_ms=1000)
            if job is None:
                continue
            keeper = asyncio.create_task(_keep_alive(queue, consumer, job, settings))
            try:
                await handle(job, settings, gateway, redis, checkpointer)
                await queue.ack(job)
            finally:
                keeper.cancel()
    await redis.aclose()
    log.info("agent worker stopped")


async def _keep_alive(queue: JobQueue, consumer: str, job: Job, settings: Settings) -> None:
    while True:  # long runs keep their job, so it isn't redelivered while still running
        await asyncio.sleep(settings.agent_job_visibility_seconds / 3)
        await queue.keep_alive(consumer, job)


def main() -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    async def go() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        await run(stop, settings)

    asyncio.run(go())


if __name__ == "__main__":
    main()
