from typing import List
import logging

from contextlib import contextmanager
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import Session
from align_data.settings import DB_CONNECTION_URI, MIN_CONFIDENCE
from align_data.db.models import Article


logger = logging.getLogger(__name__)

ENGINE = create_engine(DB_CONNECTION_URI, echo=False)


@contextmanager
def make_session(auto_commit=False):
    with Session(ENGINE).no_autoflush as session:
        yield session
        if auto_commit:
            session.commit()


def stream_pinecone_updates(
    session: Session, custom_sources: List[str], force_update: bool = False
):
    """Yield Pinecone entries that require an update."""
    yield from (
        session.query(Article)
        .filter(or_(Article.pinecone_update_required.is_(True), force_update))
        .filter(Article.is_valid)
        .filter(Article.source.in_(custom_sources))
        .filter(or_(Article.confidence == None, Article.confidence > MIN_CONFIDENCE))
        .yield_per(1000)
    )
