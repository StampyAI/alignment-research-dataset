"""date_checked column

Revision ID: cfd1704ad799
Revises: f5a2bcfa6b2c
Create Date: 2023-09-03 18:57:35.390670

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = 'cfd1704ad799'
down_revision = 'f5a2bcfa6b2c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('articles', sa.Column('date_checked', sa.DateTime(), nullable=True))
    # Set a random day in the past for the last check, so that the existing articles get checked randomly
    op.execute('UPDATE articles SET date_checked = DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 101) DAY)')
    op.alter_column('articles', 'date_checked', existing_type=mysql.DATETIME(), nullable=False)


def downgrade() -> None:
    op.drop_column('articles', 'date_checked')
