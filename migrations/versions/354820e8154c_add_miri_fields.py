"""Add MIRI fields to the articles table.

Revision ID: 354820e8154c
Revises: cfd1704ad799
Create Date: 2025-07-15 14:28:57.795635

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "354820e8154c"
down_revision = "cfd1704ad799"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "articles",
        sa.Column(
            "miri_confidence",
            sa.Float(),
            nullable=True,
            comment="How much MIRI wants this in the chatbot",
        ),
    )
    op.add_column(
        "articles",
        sa.Column(
            "miri_distance",
            sa.String(length=128),
            nullable=False,
            comment="Whether this is core or wider from MIRI's perspective",
        ),
    )
    op.add_column(
        "articles",
        sa.Column(
            "needs_tech",
            sa.Boolean(),
            nullable=False,
            comment="Whether the article is about technical details",
        ),
    )
    op.alter_column(
        "articles",
        "confidence",
        existing_type=mysql.FLOAT(),
        comment="Describes the confidence in how good this article is, as a value <0, 1>",
        existing_nullable=True,
    )
    op.alter_column(
        "articles",
        "pinecone_status",
        existing_type=mysql.VARCHAR(length=32),
        type_=sa.Enum(
            "absent",
            "pending_removal",
            "pending_addition",
            "added",
            name="pineconestatus",
        ),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "articles",
        "pinecone_status",
        existing_type=sa.Enum(
            "absent",
            "pending_removal",
            "pending_addition",
            "added",
            name="pineconestatus",
        ),
        type_=mysql.VARCHAR(length=32),
        existing_nullable=False,
    )
    op.alter_column(
        "articles",
        "confidence",
        existing_type=mysql.FLOAT(),
        comment=None,
        existing_comment="Describes the confidence in how good this article is, as a value <0, 1>",
        existing_nullable=True,
    )
    op.drop_column("articles", "needs_tech")
    op.drop_column("articles", "miri_distance")
    op.drop_column("articles", "miri_confidence")
