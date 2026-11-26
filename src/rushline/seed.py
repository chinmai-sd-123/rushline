"""Demo data for local runs and tests. Wipes existing data first."""

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection

from rushline.config import Settings
from rushline.db import create_engine

LOCAL_HOSTS = {"localhost", "127.0.0.1"}
IST = ZoneInfo("Asia/Kolkata")

USERS = [
    ("asha@example.com", "Asha"),
    ("ravi@example.com", "Ravi"),
    ("meera@example.com", "Meera"),
]
SHOW_TITLES = ["Tatkal Tuesday", "Launch Night"]
ROWS = "ABCDE"
SEATS_PER_ROW = 10
PRICE_PAISE_BY_ROW = {"A": 50000, "B": 50000, "C": 35000, "D": 35000, "E": 20000}

TRUNCATE_ALL = text(
    "TRUNCATE users, venues, shows, seats, holds, bookings, idempotency_keys, outbox "
    "RESTART IDENTITY CASCADE"
)


async def load_demo_data(conn: AsyncConnection) -> None:
    """Replace all data with the demo set. Runs inside the caller's transaction."""
    await conn.execute(TRUNCATE_ALL)
    await conn.execute(
        text("INSERT INTO users (email, name) VALUES (:email, :name)"),
        [{"email": email, "name": name} for email, name in USERS],
    )
    venue_id = (
        await conn.execute(
            text("INSERT INTO venues (name, city) VALUES (:name, :city) RETURNING id"),
            {"name": "Rushline Arena", "city": "Bengaluru"},
        )
    ).scalar_one()

    # Shows start tomorrow at 8 pm IST, one day apart, so they're always in the future.
    first_show = datetime.now(IST).replace(hour=20, minute=0, second=0, microsecond=0)
    first_show += timedelta(days=1)
    show_ids = [
        (
            await conn.execute(
                text(
                    "INSERT INTO shows (venue_id, title, starts_at) "
                    "VALUES (:venue_id, :title, :starts_at) RETURNING id"
                ),
                {"venue_id": venue_id, "title": title, "starts_at": first_show + timedelta(days=i)},
            )
        ).scalar_one()
        for i, title in enumerate(SHOW_TITLES)
    ]

    await conn.execute(
        text(
            "INSERT INTO seats (show_id, seat_no, price_paise) "
            "VALUES (:show_id, :seat_no, :price_paise)"
        ),
        [
            {"show_id": show_id, "seat_no": f"{row}{n}", "price_paise": PRICE_PAISE_BY_ROW[row]}
            for show_id in show_ids
            for row in ROWS
            for n in range(1, SEATS_PER_ROW + 1)
        ],
    )


async def seed() -> None:
    settings = Settings()
    host = make_url(settings.database_url).host
    if host not in LOCAL_HOSTS:  # never wipe a remote database
        raise SystemExit(f"Refusing to seed a non-local database (host: {host})")

    engine = create_engine(settings)
    async with engine.begin() as conn:  # one transaction: fully seeded or unchanged
        await load_demo_data(conn)
    await engine.dispose()
    seats = len(SHOW_TITLES) * len(ROWS) * SEATS_PER_ROW
    print(f"Seeded {len(USERS)} users, 1 venue, {len(SHOW_TITLES)} shows, {seats} seats")


if __name__ == "__main__":
    asyncio.run(seed())
