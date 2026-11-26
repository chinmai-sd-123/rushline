"""Job queue for agent runs: a Redis stream with a consumer group (local stand-in for SQS).

The guarantees we rely on are SQS's: at-least-once delivery, and a job whose worker
died is handed to another worker after a visibility timeout (XAUTOCLAIM). Workers
must therefore be idempotent: the agent's tool calls carry idempotency keys.
"""

import json
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

JOBS = "rushline:agent-jobs"
GROUP = "agents"


@dataclass(frozen=True)
class Job:
    id: str
    type: str
    payload: dict


class JobQueue:
    def __init__(self, redis: Redis, visibility_seconds: int = 300):
        self.redis = redis
        self.visibility_ms = visibility_seconds * 1000

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(JOBS, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # already exists: fine
                raise

    async def put(self, event_id: int, event_type: str, payload: dict) -> None:
        await self.redis.xadd(
            JOBS, {"event_id": event_id, "type": event_type, "payload": json.dumps(payload)}
        )

    async def take(self, consumer: str, block_ms: int = 1000) -> Job | None:
        # First, any job another worker took but didn't finish in time.
        _, stolen, _ = await self.redis.xautoclaim(
            JOBS, GROUP, consumer, min_idle_time=self.visibility_ms, count=1
        )
        entries = stolen or [
            entry
            for _, items in await self.redis.xreadgroup(
                GROUP, consumer, {JOBS: ">"}, count=1, block=block_ms
            )
            or []
            for entry in items
        ]
        if not entries:
            return None
        entry_id, fields = entries[0]
        return Job(
            entry_id.decode() if isinstance(entry_id, bytes) else entry_id,
            fields[b"type"].decode(),
            json.loads(fields[b"payload"]),
        )

    async def keep_alive(self, consumer: str, job: Job) -> None:
        """Reset the job's idle time, like extending an SQS visibility timeout."""
        await self.redis.xclaim(JOBS, GROUP, consumer, min_idle_time=0, message_ids=[job.id])

    async def ack(self, job: Job) -> None:
        await self.redis.xack(JOBS, GROUP, job.id)
