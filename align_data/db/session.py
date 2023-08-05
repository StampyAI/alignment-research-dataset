from typing import List, Generator
import logging

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from align_data.settings import DB_CONNECTION_URI


logger = logging.getLogger(__name__)


@contextmanager
def make_session(auto_commit=False):
    engine = create_engine(DB_CONNECTION_URI, echo=False)
    with Session(engine) as session:
        yield session
        if auto_commit:
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