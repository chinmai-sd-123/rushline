import pytest
from redis.asyncio import Redis

from rushline.agent.gateway import (
    BudgetExceeded,
    FakeProvider,
    Gateway,
    LLMReply,
    LLMUnavailable,
    cost_usd,
)
from rushline.config import Settings

MSGS = [{"role": "user", "content": "hi"}]


class Flaky:
    """Fails for some models, records which models were tried."""

    def __init__(self, failing: set[str]):
        self.failing, self.tried = failing, []

    async def chat(self, model, messages, tools):
        self.tried.append(model)
        if model in self.failing:
            raise TimeoutError(model)
        return LLMReply("ok", model=model, tokens_in=1000, tokens_out=100)


@pytest.fixture
async def redis(anyio_backend):
    r = Redis.from_url(Settings().redis_url)
    await r.flushdb()
    yield r
    await r.aclose()


def gateway(provider, redis, budget=10_000, models=("gpt-5-mini", "gpt-4.1-mini")):
    return Gateway(provider, list(models), redis, budget, timeout_seconds=1)


@pytest.mark.anyio
async def test_falls_back_to_the_next_model_on_failure(redis):
    provider = Flaky({"gpt-5-mini"})

    reply = await gateway(provider, redis).chat(1, MSGS, [])

    assert provider.tried == ["gpt-5-mini", "gpt-4.1-mini"]
    assert reply.model == "gpt-4.1-mini"
    assert reply.cost_usd == pytest.approx(cost_usd("gpt-4.1-mini", 1000, 100))


@pytest.mark.anyio
async def test_all_models_failing_raises_unavailable(redis):
    with pytest.raises(LLMUnavailable):
        await gateway(Flaky({"gpt-5-mini", "gpt-4.1-mini"}), redis).chat(1, MSGS, [])


@pytest.mark.anyio
async def test_slow_model_times_out_and_falls_back(redis):
    import asyncio

    class Slow(Flaky):
        async def chat(self, model, messages, tools):
            if model == "gpt-5-mini":
                await asyncio.sleep(5)
            return await super().chat(model, messages, tools)

    reply = await gateway(Slow(set()), redis).chat(1, MSGS, [])

    assert reply.model == "gpt-4.1-mini"


@pytest.mark.anyio
async def test_daily_token_budget_is_enforced_per_user(redis):
    gw = gateway(Flaky(set()), redis, budget=2000)
    await gw.chat(1, MSGS, [])  # 1100 tokens
    await gw.chat(1, MSGS, [])  # 2200: over the budget now

    with pytest.raises(BudgetExceeded):
        await gw.chat(1, MSGS, [])
    assert (await gw.chat(2, MSGS, [])).content == "ok"  # other users are unaffected


@pytest.mark.anyio
async def test_budget_fails_open_without_redis():
    dead = Redis.from_url("redis://localhost:1/0", socket_connect_timeout=0.2)
    reply = await gateway(Flaky(set()), dead).chat(1, MSGS, [])
    await dead.aclose()

    assert reply.content == "ok"


def test_long_conversations_route_to_the_strongest_model_first():
    gw = gateway(FakeProvider([]), None)
    long = [{"role": "user", "content": "x" * 30_000}]

    assert gw.route(MSGS) == ["gpt-5-mini", "gpt-4.1-mini"]
    assert gw.route(long) == ["gpt-4.1-mini", "gpt-5-mini"]


def test_cost_uses_the_price_table():
    assert cost_usd("gpt-5-mini", 1_000_000, 1_000_000) == pytest.approx(2.25)
    assert cost_usd("unknown-model", 10, 10) == 0.0
