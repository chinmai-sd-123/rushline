"""add booking capture attempts

Revision ID: 0b226926aec7
Revises: 1d9ffc62bd9c
Create Date: 2026-11-26 07:55:41.828119

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0b226926aec7"
down_revision: str | Sequence[str] | None = "1d9ffc62bd9c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The worker gives up on a capture after this many tries (booking -> capture_failed).
    op.execute("ALTER TABLE bookings ADD COLUMN capture_attempts integer NOT NULL DEFAULT 0")


def downgrade() -> None:
    op.execute("ALTER TABLE bookings DROP COLUMN capture_attempts")
