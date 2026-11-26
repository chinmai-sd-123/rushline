"""Tests run against a separate `rushline_test` database, rebuilt from migrations each run."""

import asyncio
import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from rushline.auth import create_token
from rushline.config import Settings
from rushline.main import app
from rushline.seed import load_demo_data

TEST_DB = "rushline_test"
ROOT = Path(__file__).resolve().parents[1]

# Configure the app for tests before any test imports it.
os.environ.setdefault("RUSHLINE_JWT_SECRET", "test-only-secret-that-is-at-least-32-chars")
os.environ["RUSHLINE_ENABLE_DEV_LOGIN"] = "true"
_base_url = make_url(Settings().database_url)
TEST_DATABASE_URL = _base_url.set(database=TEST_DB).render_as_string(hide_password=False)
os.environ["RUSHLINE_DATABASE_URL"] = TEST_DATABASE_URL


async def _recreate_test_database() -> None:
    admin = create_async_engine(
        _base_url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    async with admin.connect() as conn:
        await conn.execute(text(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)"))
        await conn.execute(text(f"CREATE DATABASE {TEST_DB}"))
    await admin.dispose()


async def _load_demo_data() -> None:
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
    async with engine.begin() as conn:
        await load_demo_data(conn)
    await engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def test_database() -> None:
    """Fresh database per run, so the full migration chain is tested every time."""
    asyncio.run(_recreate_test_database())
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
