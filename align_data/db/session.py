from typing import List, Generator
import logging

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from align_data.settings import DB_CONNECTION_URI
from align_data.db.models import Article

logger = logging.getLogger(__name__)


@contextmanager
def make_session(auto_commit: bool = False) -> Generator[Session, None, None]:
    engine = create_engine(DB_CONNECTION_URI, echo=False)
    with Session(engine, autoflush=False) as session:
        yield session
        if auto_commit:
            session.commit()


def stream_pinecone_updates(session: Session, custom_sources: List[str]) -> Generator[Article, None, None]:
    """Yield Pinecone entries that require an update."""
    yield from (
        session
        .query(Article)
        .filter(Article.pinecone_update_required.is_(True))
        .filter(Article.is_valid)
        .filter(Article.source.in_(custom_sources))
        .yield_per(1000)
    )
