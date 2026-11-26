"""index bookings by user

Revision ID: f0e6560124cf
Revises: 0b226926aec7
Create Date: 2026-11-26 08:20:12.104504

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f0e6560124cf"
down_revision: str | Sequence[str] | None = "0b226926aec7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # GET /bookings: WHERE user_id = ? ORDER BY id DESC LIMIT 100 was a seq scan (bench/plans.py).
    op.execute("CREATE INDEX bookings_user_id_id_idx ON bookings (user_id, id)")


def downgrade() -> None:
    op.execute("DROP INDEX bookings_user_id_id_idx")
