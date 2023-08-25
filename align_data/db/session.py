from typing import List
import logging

from contextlib import contextmanager
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import Session
from align_data.settings import DB_CONNECTION_URI, MIN_CONFIDENCE
from align_data.db.models import Article


logger = logging.getLogger(__name__)

# We create a single engine for the entire application
engine = create_engine(DB_CONNECTION_URI, echo=False)


@contextmanager
def make_session(auto_commit=False):
    with Session(engine, autoflush=False) as session:
        yield session
        if auto_commit:
            session.commit()


def get_pinecone_articles_to_update(
    session: Session,
    custom_sources: List[str],
    force_update: bool = False,
):
    """Yield Pinecone entries that require an update."""
    yield from (
        session.query(Article)
        .filter(or_(Article.pinecone_update_required.is_(True), force_update))
        .filter(Article.is_valid)
        .filter(Article.source.in_(custom_sources))
        .filter(or_(Article.confidence == None, Article.confidence > MIN_CONFIDENCE))
    )


def get_pinecone_articles_by_ids(
    session: Session,
    custom_sources: List[str],
    force_update: bool = False,
    hash_ids: List[int] | None = None,
):
    """Yield Pinecone entries that require an update and match the given IDs."""
    if hash_ids is None:
        hash_ids = []

    yield from (
        session.query(Article)
        .filter(or_(Article.pinecone_update_required.is_(True), force_update))
        .filter(Article.is_valid)
        .filter(Article.source.in_(custom_sources))
        .filter(or_(Article.confidence == None, Article.confidence > MIN_CONFIDENCE))
        .filter(Article.id.in_(hash_ids))
    )


def get_all_valid_article_ids(session: Session) -> List[str]:
    """Return all valid article IDs."""
    query_result = (
        session.query(Article.id)
        .filter(Article.is_valid)
        .filter(or_(Article.confidence == None, Article.confidence > MIN_CONFIDENCE))
        .all()
    )
    return [item[0] for item in query_result]
