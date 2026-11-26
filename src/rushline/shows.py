import base64
import json
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.deps import get_conn

router = APIRouter()

FIRST_PAGE = text("""
    SELECT id, venue_id, title, starts_at
    FROM shows
    ORDER BY starts_at, id
    LIMIT :limit
""")

NEXT_PAGE = text("""
    SELECT id, venue_id, title, starts_at
    FROM shows
    WHERE (starts_at, id) > (:after_starts_at, :after_id)
    ORDER BY starts_at, id
    LIMIT :limit
""")


class ShowOut(BaseModel):
    id: int
    venue_id: int
    title: str
    starts_at: datetime


class ShowPage(BaseModel):
    items: list[ShowOut]
    next_cursor: str | None


def encode_cursor(starts_at: datetime, show_id: int) -> str:
    raw = json.dumps({"starts_at": starts_at.isoformat(), "id": show_id})
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(data["starts_at"]), int(data["id"])
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="invalid cursor") from exc


@router.get("/shows")
async def list_shows(
    conn: Annotated[AsyncConnection, Depends(get_conn)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: str | None = None,
) -> ShowPage:
    if cursor is None:
        result = await conn.execute(FIRST_PAGE, {"limit": limit + 1})
    else:
        after_starts_at, after_id = decode_cursor(cursor)
        result = await conn.execute(
            NEXT_PAGE,
            {"after_starts_at": after_starts_at, "after_id": after_id, "limit": limit + 1},
        )
    rows = result.mappings().all()

    items = [ShowOut(**row) for row in rows[:limit]]
    has_more = len(rows) > limit
    next_cursor = encode_cursor(items[-1].starts_at, items[-1].id) if has_more else None
    return ShowPage(items=items, next_cursor=next_cursor)
