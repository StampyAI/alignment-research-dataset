from typing import List
import logging

from contextlib import contextmanager
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import Session
from align_data.settings import DB_CONNECTION_URI, MIN_CONFIDENCE
from align_data.db.models import Article


logger = logging.getLogger(__name__)


@contextmanager
def make_session(auto_commit=False):
    engine = create_engine(DB_CONNECTION_URI, echo=False)
    with Session(engine).no_autoflush as session:
        yield session
        if auto_commit:
            session.commit()


def stream_pinecone_updates(session, custom_sources: List[str]):
    """Yield Pinecone entries that require an update."""
    yield from (
        session
        .query(Article)
        .filter(Article.pinecone_update_required.is_(True))
        .filter(Article.is_valid)
        .filter(Article.source.in_(custom_sources))
        .filter(or_(Article.confidence == None, Article.confidence > MIN_CONFIDENCE))
        .yield_per(1000)
    )
