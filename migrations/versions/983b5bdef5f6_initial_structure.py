"""initial structure

Revision ID: 983b5bdef5f6
Revises:
Create Date: 2023-07-18 15:54:58.299651

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "983b5bdef5f6"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "articles",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("hash_id", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=1028), nullable=True),
        sa.Column("url", sa.String(length=1028), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("source_type", sa.String(length=128), nullable=True),
        sa.Column("authors", sa.String(length=1024), nullable=False),
        sa.Column("text", mysql.LONGTEXT(), nullable=True),
        sa.Column("date_published", sa.DateTime(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("date_created", sa.DateTime(), nullable=False),
        sa.Column("date_updated", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("hash_id"),
    )
    op.create_table(
        "summaries",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("source", sa.String(length=256), nullable=True),
        sa.Column("article_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["article_id"],
            ["articles.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("summaries")
    op.drop_table("articles")
