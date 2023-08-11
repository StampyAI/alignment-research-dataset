"""Add status column

Revision ID: 0d919bdacf00
Revises: 59ac3cb671e3
Create Date: 2023-08-11 16:52:45.438822

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0d919bdacf00'
down_revision = '59ac3cb671e3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('articles', sa.Column('status', sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column('articles', 'status')
