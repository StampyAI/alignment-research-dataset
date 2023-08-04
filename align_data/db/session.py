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
            
    def stream_pinecone_updates(self, custom_sources: List[str] = ['all']):
        """Yield Pinecone entries that require an update."""
        query = self.Session.query(Article).filter(Article.pinecone_update_required.is_(True))
        
        if custom_sources != ['all']:
            query = query.filter(Article.has(Article.source.in_(custom_sources)))
            
        yield from query.yield_per(1000)
        
    def get_entry_by_hash_id(self, id):
        """
        Get an entry from the articles table by hash_id.
        
        Args:
            hash_id (str): The hash_id of the entry to get.
            
        Returns:
            Article: The entry with the given hash_id, or None if no such entry exists.
        """
        with self.session_scope() as session:
            entry = session.query(Article).filter(Article.id == id).one_or_none()
            return entry

    def update_entry(self, entry):
        '''
        Update an existing entry in the articles table with the data from the given Article object.
        
        Args:
            entry (Article): The Article object with the new data.
        '''
        with self.session_scope() as session:
            session.query(Article).filter(Article.id == entry.id).update({
                'title': entry.title,
                'url': entry.url,
                'source': entry.source,
                'source_type': entry.source_type,
                'authors': entry.authors,
                'text': entry.text,
                'confidence': entry.confidence,
                'date_published': entry.date_published,
                'metadata': entry.metadata,
                'date_updated': entry.date_updated,
            })
