"""initial structure

Revision ID: 8c11b666e86f
Revises:
Create Date: 2023-07-14 15:48:49.149905

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '8c11b666e86f'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'articles',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('hash_id', sa.String(length=32), nullable=False),
        sa.Column('title', sa.String(length=1028), nullable=True),
        sa.Column('url', sa.String(length=1028), nullable=True),
        sa.Column('source', sa.String(length=128), nullable=True),
        sa.Column('source_type', sa.String(length=128), nullable=True),
        sa.Column('text', mysql.LONGTEXT(), nullable=True),
        sa.Column('date_published', sa.DateTime(), nullable=True),
        sa.Column('metadata', sa.JSON(), nullable=True),
        sa.Column('date_created', sa.DateTime(), nullable=False),
        sa.Column('date_updated', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('hash_id')
    )
    op.create_table(
        'authors',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=256), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_table(
        'author_article',
        sa.Column('article_id', sa.Integer(), nullable=False),
        sa.Column('author_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['article_id'], ['articles.id'], ),
        sa.ForeignKeyConstraint(['author_id'], ['authors.id'], ),
        sa.PrimaryKeyConstraint('article_id', 'author_id')
    )
    op.create_table(
        'summaries',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('text', sa.Text(), nullable=False),
        sa.Column('source', sa.String(length=256), nullable=True),
        sa.Column('article_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['article_id'], ['articles.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('summaries')
    op.drop_table('author_article')
    op.drop_table('authors')
    op.drop_table('articles')
