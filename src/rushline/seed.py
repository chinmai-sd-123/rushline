import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import make_url

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


async def seed() -> None:
    settings = Settings()
    host = make_url(settings.database_url).host
    if host not in LOCAL_HOSTS:
        raise SystemExit(f"Refusing to seed a non-local database (host: {host})")

    engine = create_engine(settings)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE users, venues, shows, seats, holds, bookings, "
                "idempotency_keys, outbox RESTART IDENTITY CASCADE"
            )
        )

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

        first_show = datetime.now(IST).replace(
            hour=20, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)
        show_ids = []
        for day, title in enumerate(SHOW_TITLES):
            show_id = (
                await conn.execute(
                    text(
                        "INSERT INTO shows (venue_id, title, starts_at) "
                        "VALUES (:venue_id, :title, :starts_at) RETURNING id"
                    ),
                    {
                        "venue_id": venue_id,
                        "title": title,
                        "starts_at": first_show + timedelta(days=day),
                    },
                )
            ).scalar_one()
            show_ids.append(show_id)

        seats = [
            {
                "show_id": show_id,
                "seat_no": f"{row}{number}",
                "price_paise": PRICE_PAISE_BY_ROW[row],
            }
            for show_id in show_ids
            for row in ROWS
            for number in range(1, SEATS_PER_ROW + 1)
        ]
        await conn.execute(
            text(
                "INSERT INTO seats (show_id, seat_no, price_paise) "
                "VALUES (:show_id, :seat_no, :price_paise)"
            ),
            seats,
        )

    await engine.dispose()
    print(f"Seeded {len(USERS)} users, 1 venue, {len(show_ids)} shows, {len(seats)} seats")


if __name__ == "__main__":
    asyncio.run(seed())
