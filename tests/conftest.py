"""Tests run against a separate `rushline_test` database, migrated to head before each run.

The database is reused between local runs (CREATE DATABASE is slow on slow disks).
Set RUSHLINE_TEST_FRESH_DB=1 to rebuild it. CI always starts from an empty server.
"""

import asyncio
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx2
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from rushline import mockpay, payments
from rushline.auth import create_token
from rushline.config import Settings
from rushline.main import app
from rushline.seed import load_demo_data

pytest_plugins = ["agent_helpers"]  # shared fixtures for agent tests (env)

TEST_DB = "rushline_test"
ROOT = Path(__file__).resolve().parents[1]

# Configure the app for tests before any test imports it.
os.environ.setdefault("RUSHLINE_JWT_SECRET", "test-only-secret-that-is-at-least-32-chars")
os.environ["RUSHLINE_ENABLE_DEV_LOGIN"] = "true"
os.environ["RUSHLINE_PAYMENT_TIMEOUT_SECONDS"] = "0.3"  # mock faults hang for 5s
os.environ["RUSHLINE_CAPTURE_RETRY_AFTER_SECONDS"] = "0"
_base_url = make_url(Settings().database_url)
TEST_DATABASE_URL = _base_url.set(database=TEST_DB).render_as_string(hide_password=False)
os.environ["RUSHLINE_DATABASE_URL"] = TEST_DATABASE_URL
# Redis db 1 for tests, so dev data in db 0 is never touched.
TEST_REDIS_URL = urlunsplit(urlsplit(Settings().redis_url)._replace(path="/1"))
os.environ["RUSHLINE_REDIS_URL"] = TEST_REDIS_URL


async def _prepare_test_database(fresh: bool) -> None:
    admin = create_async_engine(
        _base_url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    async with admin.connect() as conn:
        if fresh:
            await conn.execute(text(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)"))
        exists = await conn.scalar(
            text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": TEST_DB}
        )
        if not exists:
            await conn.execute(text(f"CREATE DATABASE {TEST_DB}"))
            # Test data is throwaway: don't wait for the disk on every commit.
            await conn.execute(text(f"ALTER DATABASE {TEST_DB} SET synchronous_commit = off"))
    await admin.dispose()


async def _load_demo_data() -> None:
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    async with engine.begin() as conn:
        await load_demo_data(conn)
    await engine.dispose()
    redis = Redis.from_url(TEST_REDIS_URL)
    await redis.flushdb()
    await redis.aclose()


# Payments go to the in-process mock provider, never the network.
payments.transport_override = httpx2.ASGITransport(app=mockpay.app)


@pytest.fixture(autouse=True)
def clean_mockpay() -> None:
    mockpay.payments.clear()
    mockpay.ids_by_key.clear()
    for faults in mockpay.queued.values():
        faults.clear()


@pytest.fixture(scope="session", autouse=True)
def test_database() -> None:
    """Create the test database if needed, then migrate it to head."""
    asyncio.run(_prepare_test_database(fresh=os.environ.get("RUSHLINE_TEST_FRESH_DB") == "1"))
    command.upgrade(Config(str(ROOT / "alembic.ini")), "head")


@pytest.fixture
def db() -> None:
    """Known demo data: 3 users, 2 shows, 50 seats each (ids start at 1)."""
    asyncio.run(_load_demo_data())


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def client(db):
    """App with lifespan running, on freshly seeded data."""
    with TestClient(app) as c:
        yield c


@pytest.fixture
def auth():
    """Builds an Authorization header, e.g. auth(2) or auth(1, "agent", run_id=7)."""

    def make(user_id: int = 1, actor: str = "user", run_id: int | None = None, **kw) -> dict:
        token = create_token(Settings(), user_id, actor=actor, run_id=run_id, **kw)
        return {"Authorization": f"Bearer {token}"}

    return make
