"""Venue policies for the agent: ingestion and hybrid search with citations (RAG).

Search combines meaning (pgvector cosine distance) and exact words (Postgres full-text)
with reciprocal rank fusion, so "can I bring a backpack" finds the bags section even if
the wording differs, and "re-entry" still matches exactly.

    uv run python -m rushline.policies ingest data/policies
"""

import asyncio
import re
import sys
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.auth import require_actor
from rushline.deps import get_conn
from rushline.embeddings import Embedder, make_embedder, to_pgvector

router = APIRouter(prefix="/policies")

RRF_K = 60  # standard reciprocal rank fusion constant
CANDIDATES = 20  # per method, before fusion

SEARCH = text(f"""
    WITH vec AS (
        SELECT id, row_number() OVER (ORDER BY embedding <=> CAST(:embedding AS vector)) AS rank
        FROM policy_chunks
        WHERE venue_id IS NULL OR venue_id = :venue_id
        ORDER BY embedding <=> CAST(:embedding AS vector)
        LIMIT {CANDIDATES}
    ),
    fts AS (
        -- rank(), not row_number(): equal text scores share a rank instead of an
        -- arbitrary order, so the vector side decides between them.
        SELECT id, rank() OVER (ORDER BY ts_rank_cd(tsv, query) DESC) AS rank
        -- OR the words: questions rarely share every word with the answer; ranking sorts it out.
        FROM policy_chunks,
             to_tsquery('english', replace(plainto_tsquery('english', :q)::text, '&', '|'))
                 AS query
        WHERE tsv @@ query AND (venue_id IS NULL OR venue_id = :venue_id)
        ORDER BY ts_rank_cd(tsv, query) DESC
        LIMIT {CANDIDATES}
    )
    SELECT c.id, c.doc, c.section, c.text,
           coalesce(1.0 / ({RRF_K} + vec.rank), 0) + coalesce(1.0 / ({RRF_K} + fts.rank), 0)
               AS score
    FROM policy_chunks c
    LEFT JOIN vec ON vec.id = c.id
    LEFT JOIN fts ON fts.id = c.id
    WHERE vec.id IS NOT NULL OR fts.id IS NOT NULL
    ORDER BY score DESC, vec.rank NULLS LAST  -- on a tie, trust meaning over shared words
    LIMIT :k
""")
UPSERT = text("""
    INSERT INTO policy_chunks (venue_id, doc, section, text, embedding)
    VALUES (:venue_id, :doc, :section, :text, CAST(:embedding AS vector))
    ON CONFLICT (doc, section) DO UPDATE
    SET venue_id = EXCLUDED.venue_id, text = EXCLUDED.text, embedding = EXCLUDED.embedding
""")


class PolicyHit(BaseModel):
    chunk_id: int
    doc: str
    section: str
    text: str
    score: float
    citation: str  # what the agent must quote back, e.g. "Entry and security § Bags"


class PolicyResults(BaseModel):
    results: list[PolicyHit]


async def search(
    conn: AsyncConnection, embedder: Embedder, q: str, venue_id: int | None, k: int
) -> list[PolicyHit]:
    [embedding] = await embedder.embed([q])
    rows = await conn.execute(
        SEARCH, {"embedding": to_pgvector(embedding), "q": q, "venue_id": venue_id, "k": k}
    )
    return [
        PolicyHit(
            chunk_id=r.id,
            doc=r.doc,
            section=r.section,
            text=r.text,
            score=float(r.score),
            citation=f"{r.doc} § {r.section}",
        )
        for r in rows
    ]


@router.get("/search")
async def search_policies(
    request: Request,
    conn: Annotated[AsyncConnection, Depends(get_conn)],
    _: Annotated[object, Depends(require_actor("user", "agent", "mcp"))],
    q: Annotated[str, Query(min_length=2, max_length=300)],
    venue_id: int | None = None,
    k: Annotated[int, Query(ge=1, le=10)] = 4,
) -> PolicyResults:
    hits = await search(conn, request.app.state.embedder, q, venue_id, k)
    return PolicyResults(results=hits)


# --- Ingestion ---


def chunk_markdown(markdown: str) -> tuple[str, str | None, list[tuple[str, str]]]:
    """Split a policy doc into (title, venue name, [(section, text)]), one chunk per ## section."""
    title = re.search(r"^# (.+)$", markdown, re.M)
    venue = re.search(r"^Venue: (.+)$", markdown, re.M)
    sections = re.split(r"^## ", markdown, flags=re.M)[1:]
    chunks = []
    for section in sections:
        heading, _, body = section.partition("\n")
        chunks.append((heading.strip(), " ".join(body.split())))
    return (
        (title.group(1).strip() if title else "Untitled"),
        (venue.group(1).strip() if venue else None),
        chunks,
    )


async def ingest(conn: AsyncConnection, embedder: Embedder, folder: Path) -> int:
    """Upsert every section of every .md file. Runs inside the caller's transaction."""
    count = 0
    for path in sorted(folder.glob("*.md")):
        doc, venue_name, chunks = chunk_markdown(path.read_text())
        venue_id = None
        if venue_name:
            venue_id = await conn.scalar(
                text("SELECT id FROM venues WHERE name = :name"), {"name": venue_name}
            )
        embeddings = await embedder.embed([f"{doc}: {s}. {t}" for s, t in chunks])
        for (section, body), embedding in zip(chunks, embeddings, strict=True):
            await conn.execute(
                UPSERT,
                {
                    "venue_id": venue_id,
                    "doc": doc,
                    "section": section,
                    "text": body,
                    "embedding": to_pgvector(embedding),
                },
            )
            count += 1
    return count


async def _cli(folder: str) -> None:
    from rushline.config import Settings
    from rushline.db import create_engine

    settings = Settings()
    engine, embedder = create_engine(settings), make_embedder(settings)
    async with engine.begin() as conn:
        count = await ingest(conn, embedder, Path(folder))
    await engine.dispose()
    print(f"Ingested {count} policy sections with {embedder.name}")


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "ingest":
        raise SystemExit("usage: python -m rushline.policies ingest <folder>")
    asyncio.run(_cli(sys.argv[2]))
