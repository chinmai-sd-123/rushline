import base64
import json
from datetime import datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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


SEAT_MAP = text("""
    SELECT id, seat_no, price_paise, status
    FROM seats
    WHERE show_id = :show_id
    ORDER BY id
""")

SHOW_EXISTS = text("SELECT 1 FROM shows WHERE id = :show_id")


class ShowOut(BaseModel):
    id: int
    venue_id: int
    title: str
    starts_at: datetime


class ShowPage(BaseModel):
    items: list[ShowOut]
    next_cursor: str | None


class SeatOut(BaseModel):
    id: int
    seat_no: str
    price_paise: int
    status: Literal["available", "held", "booked"]


class SeatMap(BaseModel):
    show_id: int
    seats: list[SeatOut]


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


async def load_seat_map(engine: AsyncEngine, show_id: int) -> dict:
    """Read the seat map from Postgres with its own connection (it may run in the background)."""
    async with engine.connect() as conn:
        rows = (await conn.execute(SEAT_MAP, {"show_id": show_id})).mappings().all()
        # Every show has seats, so only check existence when nothing came back.
        if not rows and (await conn.execute(SHOW_EXISTS, {"show_id": show_id})).first() is None:
            raise HTTPException(status_code=404, detail="show not found")
    return SeatMap(show_id=show_id, seats=[SeatOut(**row) for row in rows]).model_dump()


@router.get("/shows/{show_id}/seats", response_model=SeatMap)
async def seat_map(show_id: int, request: Request) -> JSONResponse:
    """Cached for about a second (cache.py). Hits never touch Postgres or the pool."""
    engine = request.app.state.engine
    if not request.app.state.settings.seat_map_cache:
        return JSONResponse(await load_seat_map(engine, show_id), headers={"X-Cache": "off"})
    data, cache_status = await request.app.state.seat_maps.get(
        show_id, lambda: load_seat_map(engine, show_id)
    )
    return JSONResponse(data, headers={"X-Cache": cache_status})
