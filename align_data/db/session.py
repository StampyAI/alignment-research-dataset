from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from align_data.settings import DB_CONNECTION_URI


@contextmanager
def make_session(auto_commit=False):
    engine = create_engine(DB_CONNECTION_URI, echo=False)
    with Session(engine).no_autoflush as session:
        yield session
        if auto_commit:
            session.commit()
