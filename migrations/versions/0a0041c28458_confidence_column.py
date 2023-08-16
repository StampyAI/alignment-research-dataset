"""confidence column

Revision ID: 0a0041c28458
Revises: 983b5bdef5f6
Create Date: 2023-07-24 15:36:16.233887

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0a0041c28458"
down_revision = "983b5bdef5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("articles", sa.Column("confidence", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("articles", "confidence")
