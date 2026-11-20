"""create holds and seats

Revision ID: f13b1355b380
Revises: cdffb63c1377
Create Date: 2026-11-20 05:34:22.708706

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f13b1355b380"
down_revision: str | Sequence[str] | None = "cdffb63c1377"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE holds (
            id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            user_id          bigint NOT NULL REFERENCES users (id),
            show_id          bigint NOT NULL REFERENCES shows (id),
            status           text NOT NULL
                             CHECK (status IN ('active', 'paying', 
                             'converted', 'expired', 'released')),
            expires_at       timestamptz NOT NULL,
            payment_deadline timestamptz,
            created_at       timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX holds_one_open_per_user_show
        ON holds (user_id, show_id)
        WHERE status IN ('active', 'paying')
    """)
    op.execute("""
        CREATE INDEX holds_open_expires_at_idx
        ON holds (expires_at)
        WHERE status IN ('active', 'paying')
    """)
    op.execute("""
        CREATE TABLE seats (
            id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            show_id     bigint NOT NULL REFERENCES shows (id),
            seat_no     text NOT NULL,
            price_paise integer NOT NULL CHECK (price_paise >= 0),
            status      text NOT NULL DEFAULT 'available'
                        CHECK (status IN ('available', 'held', 'booked')),
            hold_id     bigint REFERENCES holds (id),
            version     integer NOT NULL DEFAULT 0,
            UNIQUE (show_id, seat_no),
            CHECK ((status = 'available') = (hold_id IS NULL))
        )
    """)
    op.execute("CREATE INDEX seats_show_id_status_idx ON seats (show_id, status)")
    op.execute("CREATE INDEX seats_hold_id_idx ON seats (hold_id) WHERE hold_id IS NOT NULL")


def downgrade() -> None:
    op.execute("DROP TABLE seats")
    op.execute("DROP TABLE holds")
