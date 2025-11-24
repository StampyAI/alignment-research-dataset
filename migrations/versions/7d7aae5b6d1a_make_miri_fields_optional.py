"""Make MIRI fields optional.

Revision ID: 7d7aae5b6d1a
Revises: 354820e8154c
Create Date: 2025-11-21 20:53:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7d7aae5b6d1a"
down_revision = "354820e8154c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "articles",
        "miri_distance",
        existing_type=sa.String(length=128),
        nullable=True,
    )
    op.alter_column(
        "articles",
        "needs_tech",
        existing_type=sa.Boolean(),
        nullable=True,
    )


def downgrade() -> None:
    # Backfill nulls so the NOT NULL constraint can be re-applied safely.
    op.execute("UPDATE articles SET miri_distance = '' WHERE miri_distance IS NULL")
    op.execute("UPDATE articles SET needs_tech = 0 WHERE needs_tech IS NULL")

    op.alter_column(
        "articles",
        "miri_distance",
        existing_type=sa.String(length=128),
        nullable=False,
    )
    op.alter_column(
        "articles",
        "needs_tech",
        existing_type=sa.Boolean(),
        nullable=False,
    )
