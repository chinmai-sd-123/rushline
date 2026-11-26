import asyncio
import os
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from rushline.embeddings import HashEmbedder
from rushline.policies import chunk_markdown, ingest

POLICY_DIR = Path(__file__).resolve().parents[1] / "data" / "policies"


@pytest.fixture
def policies(db):
    async def go():
        engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
        async with engine.begin() as conn:
            count = await ingest(conn, HashEmbedder(), POLICY_DIR)
        await engine.dispose()
        return count

    return asyncio.run(go())


def search(client, auth, q, **params):
    response = client.get("/policies/search", params={"q": q, **params}, headers=auth())
    assert response.status_code == 200, response.text
    return response.json()["results"]


def test_markdown_is_split_into_sections():
    title, venue, chunks = chunk_markdown((POLICY_DIR / "rushline-arena.md").read_text())

    assert title == "Rushline Arena house rules"
    assert venue == "Rushline Arena"
    assert [s for s, _ in chunks] == ["Location", "Parking", "Re-entry"]


def test_ingest_is_idempotent(policies, client, auth):
    assert policies == 14  # 4 + 4 + 3 + 3 sections
    assert asyncio.run(_count_after_reingest()) == 14


async def _count_after_reingest():
    from sqlalchemy import text

    engine = create_async_engine(os.environ["RUSHLINE_DATABASE_URL"], poolclass=NullPool)
    async with engine.begin() as conn:
        await ingest(conn, HashEmbedder(), POLICY_DIR)
        count = await conn.scalar(text("SELECT count(*) FROM policy_chunks"))
    await engine.dispose()
    return count


@pytest.mark.parametrize(
    ("question", "citation"),
    [
        ("can I bring my backpack", "Entry and security § Bags"),
        ("is there parking at the arena", "Rushline Arena house rules § Parking"),
        ("how do refunds work", "Ticketing and refunds § Refunds"),
        ("wheelchair spaces", "Accessibility § Wheelchair spaces"),
        # Needs word overlap: the offline hashing embedder can't match "leave and come back
        # in" to "re-entry". That's a job for real embeddings (live evals with OpenAI).
        ("is re-entry allowed", "Rushline Arena house rules § Re-entry"),
    ],
)
def test_search_finds_the_right_section_with_a_citation(policies, client, auth, question, citation):
    results = search(client, auth, question, venue_id=1)

    assert citation in [r["citation"] for r in results[:3]]


def test_venue_rules_are_scoped_to_their_venue(policies, client, auth):
    other_venue = search(client, auth, "parking at the arena", venue_id=999)

    assert all(not r["doc"].startswith("Rushline Arena") for r in other_venue)


def test_search_needs_a_token(client):
    assert client.get("/policies/search", params={"q": "bags"}).status_code == 401
