"""LLM gateway: routing, fallback, per-user token budgets and cost tracking.

A module inside the agent worker, not a service (design: one consumer for now).
Providers are swappable: OpenAI in production, a scripted fake in tests and evals.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

# USD per 1M tokens (input, output), from OpenAI's published prices when this was written.
# Prices change: check https://openai.com/api/pricing before trusting cost reports.
PRICES_PER_MILLION = {
    "gpt-5-mini": (0.25, 2.00),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4o-mini": (0.15, 0.60),
    "fake": (0.0, 0.0),
}
LONG_CONTEXT_CHARS = 24_000  # roughly 6k tokens: past this, route to the stronger model first


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass
class LLMReply:
    content: str | None
    tool_calls: list[ToolCall] = field(default_factory=list)
    model: str = ""
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0


class Provider(Protocol):
    async def chat(self, model: str, messages: list[dict], tools: list[dict]) -> LLMReply: ...


class BudgetExceeded(Exception):
    pass


class LLMUnavailable(Exception):
    pass


def cost_usd(model: str, tokens_in: int, tokens_out: int) -> float:
    price_in, price_out = PRICES_PER_MILLION.get(model, (0.0, 0.0))
    return (tokens_in * price_in + tokens_out * price_out) / 1_000_000


class OpenAIProvider:
    def __init__(self, api_key: str, timeout_seconds: float):
        from openai import AsyncOpenAI  # imported here so tests never need the SDK configured

        # Retries are the gateway's job (it can switch models), so the SDK doesn't retry.
        self.client = AsyncOpenAI(api_key=api_key, timeout=timeout_seconds, max_retries=0)

    async def chat(self, model: str, messages: list[dict], tools: list[dict]) -> LLMReply:
        response = await self.client.chat.completions.create(
            model=model, messages=messages, tools=tools or None
        )
        message = response.choices[0].message
        calls = [
            ToolCall(c.id, c.function.name, json.loads(c.function.arguments or "{}"))
            for c in message.tool_calls or []
        ]
        usage = response.usage
        tokens_in = usage.prompt_tokens if usage else 0
        tokens_out = usage.completion_tokens if usage else 0
        return LLMReply(message.content, calls, model, tokens_in, tokens_out)


class FakeProvider:
    """Scripted replies for tests and evals: a list of replies, or a function of the messages."""

    def __init__(self, script: list[LLMReply] | Callable[[list[dict]], LLMReply]):
        self.script = script
        self.calls: list[list[dict]] = []

    async def chat(self, model: str, messages: list[dict], tools: list[dict]) -> LLMReply:
        self.calls.append(messages)
        reply = self.script(messages) if callable(self.script) else self.script.pop(0)
        reply.model = reply.model or model
        return reply


class Gateway:
    def __init__(
        self,
        provider: Provider,
        models: list[str],
        redis: Redis | None,
        daily_tokens_per_user: int,
        timeout_seconds: float,
    ):
        self.provider = provider
        self.models = models  # cheapest first; later ones are fallbacks
        self.redis = redis
        self.daily_tokens = daily_tokens_per_user
        self.timeout = timeout_seconds

    def route(self, messages: list[dict]) -> list[str]:
        """Cheap model first; long conversations go to the strongest model first."""
        if len(self.models) > 1 and len(json.dumps(messages)) > LONG_CONTEXT_CHARS:
            return [self.models[-1], *self.models[:-1]]
        return list(self.models)

    async def chat(self, user_id: int, messages: list[dict], tools: list[dict]) -> LLMReply:
        await self._check_budget(user_id)
        errors = []
        for model in self.route(messages):
            try:
                reply = await asyncio.wait_for(
                    self.provider.chat(model, messages, tools), self.timeout
                )
            except Exception as exc:  # timeouts, rate limits, 5xx: try the next model
                errors.append(f"{model}: {type(exc).__name__}")
                log.warning("llm call failed, falling back", extra={"fields": {"model": model}})
                continue
            reply.cost_usd = cost_usd(reply.model, reply.tokens_in, reply.tokens_out)
            await self._charge(user_id, reply.tokens_in + reply.tokens_out)
            return reply
        raise LLMUnavailable("; ".join(errors))

    def _budget_key(self, user_id: int) -> str:
        return f"llm:tokens:{user_id}:{datetime.now(UTC):%Y%m%d}"

    async def _redis(self, call: Callable[[], Awaitable]):
        # Budgets fail open if Redis is down: the per-run budget still caps every run.
        if self.redis is None:
            return None
        try:
            return await call()
        except (RedisError, OSError):
            log.warning("token budget unavailable")
            return None

    async def _check_budget(self, user_id: int) -> None:
        used = await self._redis(lambda: self.redis.get(self._budget_key(user_id)))
        if used is not None and int(used) >= self.daily_tokens:
            raise BudgetExceeded(f"daily token budget of {self.daily_tokens} used up")

    async def _charge(self, user_id: int, tokens: int) -> None:
        key = self._budget_key(user_id)

        async def charge():
            async with self.redis.pipeline(transaction=True) as pipe:
                await pipe.incrby(key, tokens).expire(key, 2 * 24 * 3600).execute()

        await self._redis(charge)
