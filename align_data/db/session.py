from typing import List, Generator
import logging

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.dialects.mysql import insert

from align_data.db.models import Article, Summary
from align_data.settings import DB_CONNECTION_URI


logger = logging.getLogger(__name__)


class MySQLDB:
    def __init__(self, connection_uri=DB_CONNECTION_URI):
        """Initialize the MySQLDB with the database connection URI."""
        self.engine = create_engine(connection_uri)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"An error occurred: {str(e)}")
            raise
        finally:
            session.close()
            
    def stream_pinecone_updates(self, custom_sources: List[str]):
        """Yield Pinecone entries that require an update."""
        query = self.Session.query(Article).filter(Article.pinecone_update_required.is_(True))
        query = query.filter(Article.source.in_(custom_sources))
        yield from query.yield_per(1000)