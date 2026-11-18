"""create users venues shows

Revision ID: cdffb63c1377
Revises:
Create Date: 2026-11-18 10:36:38.869833

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "cdffb63c1377"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE users (
            id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            email      text NOT NULL UNIQUE,
            name       text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TABLE venues (
            id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name       text NOT NULL,
            city       text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TABLE shows (
            id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            venue_id   bigint NOT NULL REFERENCES venues (id),
            title      text NOT NULL,
            starts_at  timestamptz NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX shows_venue_id_idx ON shows (venue_id)")
    op.execute("CREATE INDEX shows_starts_at_id_idx ON shows (starts_at, id)")


def downgrade() -> None:
    op.execute("DROP TABLE shows")
    op.execute("DROP TABLE venues")
    op.execute("DROP TABLE users")
