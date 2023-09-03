"""pinecone status

Revision ID: 1866340e456a
Revises: f5a2bcfa6b2c
Create Date: 2023-09-03 15:34:02.755588

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = '1866340e456a'
down_revision = 'f5a2bcfa6b2c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    ## Set the pinecone status
    op.add_column('articles', sa.Column('pinecone_status', sa.String(length=32), nullable=False))

    IS_VALID = """(
       articles.status IS NULL AND
       articles.text IS NOT NULL AND
       articles.url IS NOT NULL AND
       articles.title IS NOT NULL AND
       articles.authors IS NOT NULL
    )"""
    op.execute(f"""
      UPDATE articles SET pinecone_status = 'absent'
      WHERE NOT articles.pinecone_update_required AND NOT {IS_VALID}
    """)
    op.execute(f"""
      UPDATE articles SET pinecone_status = 'pending_removal'
      WHERE articles.pinecone_update_required AND NOT {IS_VALID}
    """)
    op.execute(f"""
      UPDATE articles SET pinecone_status = 'pending_addition'
      WHERE articles.pinecone_update_required AND {IS_VALID}
    """)
    op.execute(f"""
      UPDATE articles SET pinecone_status = 'added'
      WHERE NOT articles.pinecone_update_required AND {IS_VALID}
    """)

    op.drop_column('articles', 'pinecone_update_required')


def downgrade() -> None:
    op.add_column("articles", sa.Column("pinecone_update_required", sa.Boolean(), nullable=False))
    op.execute("UPDATE articles SET articles.pinecone_update_required = (pinecone_status = 'pending_addition')")
    op.drop_column('articles', 'pinecone_status')
