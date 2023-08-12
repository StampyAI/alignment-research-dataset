"""Add status column

Revision ID: f5a2bcfa6b2c
Revises: 59ac3cb671e3
Create Date: 2023-08-12 15:59:44.741360

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = 'f5a2bcfa6b2c'
down_revision = '59ac3cb671e3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('articles', sa.Column('status', sa.String(length=256), nullable=True))
    op.add_column('articles', sa.Column('comments', mysql.LONGTEXT(), nullable=True))


def downgrade() -> None:
    op.drop_column('articles', 'comments')
    op.drop_column('articles', 'status')
