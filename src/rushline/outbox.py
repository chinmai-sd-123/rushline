"""Transactional outbox: events are written in the same transaction as the change
they describe, then published by the worker (at least once, so consumers dedupe by id).
"""

import json

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.queue import JobQueue

STREAM = "rushline:events"
STREAM_MAX_LEN = 10_000  # approximate cap so the stream can't grow forever

INSERT = text("INSERT INTO outbox (event_type, payload) VALUES (:type, CAST(:payload AS jsonb))")


async def add(conn: AsyncConnection, event_type: str, payload: dict) -> None:
    """Call inside the caller's transaction."""
    await conn.execute(INSERT, {"type": event_type, "payload": json.dumps(payload)})


class RedisStreamPublisher:
    """Local stand-in for SQS. Agent run events (run.*) go to the job queue the agent
    workers consume. Everything else goes to the events stream."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self.jobs = JobQueue(redis)

    async def publish(self, event_id: int, event_type: str, payload: dict) -> None:
        if event_type.startswith("run."):
            await self.jobs.put(event_id, event_type, payload)
            return
        await self.redis.xadd(
            STREAM,
            {"id": event_id, "type": event_type, "payload": json.dumps(payload)},
            maxlen=STREAM_MAX_LEN,
            approximate=True,
        )
