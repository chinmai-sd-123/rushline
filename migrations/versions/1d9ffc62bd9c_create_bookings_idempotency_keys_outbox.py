"""create bookings idempotency keys outbox

Revision ID: 1d9ffc62bd9c
Revises: f13b1355b380
Create Date: 2026-11-20 07:10:43.643490

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1d9ffc62bd9c"
down_revision: str | Sequence[str] | None = "f13b1355b380"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        CREATE TABLE bookings (
            id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            user_id      bigint NOT NULL REFERENCES users (id),
            hold_id      bigint NOT NULL UNIQUE REFERENCES holds (id),
            amount_paise integer NOT NULL CHECK (amount_paise >= 0),
            status       text NOT NULL DEFAULT 'pending_capture'
                         CHECK (status IN ('pending_capture', 
                         'confirmed', 'capture_failed', 'cancelled')),
            payment_ref  text,
            created_at   timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX bookings_pending_capture_idx
        ON bookings (created_at)
        WHERE status = 'pending_capture'
    """)
    op.execute("""
        CREATE TABLE idempotency_keys (
            user_id         bigint NOT NULL REFERENCES users (id),
            key             text NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
            request_hash    text NOT NULL,
            response_status integer,
            response_body   jsonb,
            created_at      timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, key)
        )
    """)
    op.execute("""
        CREATE TABLE outbox (
            id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            event_type   text NOT NULL,
            payload      jsonb NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            published_at timestamptz
        )
    """)
    op.execute("""
        CREATE INDEX outbox_unpublished_idx
        ON outbox (id)
        WHERE published_at IS NULL
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TABLE outbox")
    op.execute("DROP TABLE idempotency_keys")
    op.execute("DROP TABLE bookings")
