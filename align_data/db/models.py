import json
import logging
import pytz
import hashlib
from datetime import datetime
from typing import List, Optional
from sqlalchemy import JSON, DateTime, ForeignKey, String, Boolean, Text, Float, func, event
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy.dialects.mysql import LONGTEXT


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Base(DeclarativeBase):
    pass


class Summary(Base):

    __tablename__ = "summaries"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(256))
    article_id: Mapped[str] = mapped_column(ForeignKey("articles.id"))

    article: Mapped["Article"] = relationship(back_populates="summaries")


class Article(Base):
    
    __tablename__ = "articles"

    _id: Mapped[int] = mapped_column('id', primary_key=True)
    id: Mapped[str] = mapped_column('hash_id', String(32), unique=True, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(1028))
    url: Mapped[Optional[str]] = mapped_column(String(1028))
    source: Mapped[Optional[str]] = mapped_column(String(128))
    source_type: Mapped[Optional[str]] = mapped_column(String(128))
    authors: Mapped[str] = mapped_column(String(1024))
    text: Mapped[Optional[str]] = mapped_column(LONGTEXT)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    date_published: Mapped[Optional[datetime]]
    meta: Mapped[Optional[JSON]] = mapped_column(JSON, name='metadata', default='{}')
    date_created: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    date_updated: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=func.current_timestamp())
    
    pinecone_update_required: Mapped[bool] = mapped_column(Boolean, default=False)
    pinecone_delete_required: Mapped[bool] = mapped_column(Boolean, default=False)
    incomplete: Mapped[bool] = mapped_column(Boolean, default=False)
    
    summaries: Mapped[List[Summary]] = relationship("Summary", back_populates="article", cascade="all, delete-orphan")

    __id_fields = ['title', 'url']

    def __init__(self, *args, id_fields, **kwargs):
        self.__id_fields = id_fields
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Article(id={self.id!r}, name={self.title!r}, fullname={self.url!r}, source={self.source!r}, source_type={self.source_type!r}, authors={self.authors!r}, date_published={self.date_published!r}, pinecone_update_required={self.pinecone_update_required!r}, pinecone_delete_required={self.pinecone_delete_required!r}, incomplete={self.incomplete!r})"
    
    def __eq__(self, other):
        if not isinstance(other, Article):
            return NotImplemented
        return (
            self.id == other.id and
            self.title == other.title and
            self.url == other.url and
            self.source == other.source and
            self.source_type == other.source_type and
            self.authors == other.authors and
            self.date_published == other.date_published and
            self.text == other.text
        )

    def generate_id_string(self):
        return ''.join(str(getattr(self, field)) for field in self.__id_fields).encode("utf-8")

    def verify_fields(self):
        missing = [field for field in self.__id_fields if not getattr(self, field)]
        if missing:
            logger.warning(f'Entry is missing the following fields: {missing}')
            return False
        return True

    @classmethod
    def before_write(cls, mapper, connection, target):
        session = Session(connection)

        # Check if an Article with the same id already exists
        db_article = session.query(Article).filter(Article.id == target.id).one_or_none()
        if db_article is not None:
            # Compare fields and update if necessary
            for field in ['title', 'url', 'source', 'source_type', 'authors', 'text', 'date_published', 'meta']:
                if getattr(db_article, field) != getattr(target, field):
                    setattr(db_article, field, getattr(target, field))
                    db_article.pinecone_update_required = True
            return

        # Verify required fields
        if target.verify_fields():
            target.incomplete = False
            target.pinecone_update_required = True
        else:
            target.incomplete = True

        # Generate id if necessary
        if target.id is None:
            id_string = target.generate_id_string()
            target.id = hashlib.md5(id_string).hexdigest()

    def to_dict(self):
        if date := self.date_published:
            date = date.replace(tzinfo=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta = json.loads(self.meta) if isinstance(self.meta, str) else self.meta
        return {
            'id': self.id,
            'title': self.title,
            'url': self.url,
            'source': self.source,
            'source_type': self.source_type,
            'text': self.text,
            'date_published': date,
            'authors': [i.strip() for i in self.authors.split(',')] if self.authors.strip() else [],
            'summaries': [s.text for s in self.summaries],
            **meta,
        }


event.listen(Article, 'before_insert', Article.before_write)
event.listen(Article, 'before_update', Article.before_write)
