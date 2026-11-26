"""Seat map cache (design: reads vastly outnumber writes, p99 < 200 ms).

An entry is fresh for `fresh_seconds`, then served stale while one caller refreshes it,
and dropped after `stale_seconds`. A cold cache under a spike costs one database read:
callers in this process share one in-flight load, and other processes see a short
Redis lock and wait for its result. Redis never decides availability: holds always
check Postgres. If Redis is down, every read goes to Postgres.
"""

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Literal

from redis.asyncio import Redis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

Loader = Callable[[], Awaitable[dict]]
CacheStatus = Literal["hit", "stale", "miss", "bypass"]

# Delete the lock only if we still own it (it may have expired and been retaken).
_RELEASE = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end return 0"
)


class SeatMapCache:
    def __init__(
        self,
        redis: Redis,
        fresh_seconds: float = 1.0,
        stale_seconds: float = 10.0,
        lock_ms: int = 2000,
        wait_for_peer_seconds: float = 0.5,
    ):
        self.redis = redis
        self.fresh = fresh_seconds
        self.stale = stale_seconds
        self.lock_ms = lock_ms
        self.wait_for_peer = wait_for_peer_seconds
        self.loads = 0  # database loads, for tests and benchmarks
        self._inflight: dict[int, asyncio.Future] = {}
        self._background: set[asyncio.Task] = set()

    @staticmethod
    def _key(show_id: int) -> str:
        return f"seatmap:{show_id}"

    async def get(self, show_id: int, load: Loader) -> tuple[dict, CacheStatus]:
        try:
            raw = await self.redis.get(self._key(show_id))
        except (RedisError, OSError):
            # Redis is slow or down: still one load per show, or a spike becomes a DB stampede.
            return await self._single_flight(show_id, load, use_redis=False), "bypass"
        if raw is None:
            return await self._single_flight(show_id, load), "miss"
        entry = json.loads(raw)
        if entry["fresh_until"] > time.time():
            return entry["data"], "hit"
        if show_id not in self._inflight:  # serve stale now, refresh once in the background
            task = asyncio.create_task(self._single_flight(show_id, load))
            self._background.add(task)
            task.add_done_callback(self._background.discard)
        return entry["data"], "stale"

    async def _single_flight(self, show_id: int, load: Loader, use_redis: bool = True) -> dict:
        """Everyone in this process asking for the same show waits on one load."""
        if show_id in self._inflight:
            return await asyncio.shield(self._inflight[show_id])
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(lambda f: f.cancelled() or f.exception())  # no "never retrieved"
        self._inflight[show_id] = future
        try:
            if use_redis:
                data = await self._load_across_processes(show_id, load)
            else:
                data = await self._load(load)
            future.set_result(data)
            return data
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            del self._inflight[show_id]

    async def _load_across_processes(self, show_id: int, load: Loader) -> dict:
        key, lock, token = self._key(show_id), f"{self._key(show_id)}:lock", uuid.uuid4().hex
        try:
            owner = await self.redis.set(lock, token, nx=True, px=self.lock_ms)
        except (RedisError, OSError):
            return await self._load(load)  # Redis is down: just read Postgres
        if not owner:  # another process is loading: wait a moment for its result
            data = await self._wait_for_peer(key)
            if data is not None:
                return data
            # Too slow or it crashed: load ourselves. Correctness beats saving a query.
        try:
            data = await self._load(load)
            await self._redis_quietly(
                self.redis.set(
                    key,
                    json.dumps({"fresh_until": time.time() + self.fresh, "data": data}),
                    px=int(self.stale * 1000),
                )
            )
            return data
        finally:
            if owner:
                await self._redis_quietly(self.redis.eval(_RELEASE, 1, lock, token))

    async def _wait_for_peer(self, key: str) -> dict | None:
        deadline = time.monotonic() + self.wait_for_peer
        while time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            raw = await self._redis_quietly(self.redis.get(key))
            if raw is not None:
                return json.loads(raw)["data"]
        return None

    @staticmethod
    async def _redis_quietly(call: Awaitable):
        """A cache write or poll failing must never fail the request."""
        try:
            return await call
        except (RedisError, OSError):
            log.warning("seat map cache unavailable")
            return None

    async def _load(self, load: Loader) -> dict:
        self.loads += 1
        return await load()

    async def aclose(self) -> None:
        """Let background refreshes finish before shutdown."""
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
