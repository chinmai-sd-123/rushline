"""Per-user rate limits: fixed one-minute windows counted in Redis.

If Redis is unreachable the API keeps serving, with a stricter per-process limit,
because one process can't see the other copies' counts (design: Risks).
"""

import time
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from redis.exceptions import RedisError

from rushline.auth import Principal, current_principal

WINDOW_SECONDS = 60


def _now() -> float:
    return time.time()  # its own function, so tests can freeze it without patching time


class _LocalWindows:
    """Fallback counter for when Redis is down. Keeps only the current window."""

    def __init__(self) -> None:
        self.window = -1
        self.counts: dict[str, int] = {}

    def hit(self, window: int, key: str) -> int:
        if window != self.window:
            self.window, self.counts = window, {}
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]


_local = _LocalWindows()


def rate_limit(name: str):
    """Dependency factory: limit is read from settings as `<name>_per_minute`."""

    async def check(
        request: Request, principal: Annotated[Principal, Depends(current_principal)]
    ) -> None:
        limit = getattr(request.app.state.settings, f"{name}_per_minute")
        now = _now()
        window = int(now // WINDOW_SECONDS)
        key = f"rl:{name}:{principal.user_id}:{window}"
        try:
            async with request.app.state.redis.pipeline(transaction=True) as pipe:
                count, _ = await pipe.incr(key).expire(key, WINDOW_SECONDS * 2).execute()
        except (RedisError, OSError):
            count = _local.hit(window, key)
            limit = max(1, limit // 2)  # stricter while we can't see other copies
        if count > limit:
            retry_after = WINDOW_SECONDS - int(now) % WINDOW_SECONDS
            raise HTTPException(
                429, "rate limit exceeded", headers={"Retry-After": str(retry_after)}
            )

    return check
