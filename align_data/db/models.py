import json
import logging
import pytz
import hashlib
from datetime import datetime
from typing import List, Optional
from sqlalchemy import JSON, DateTime, ForeignKey, String, Boolean, Text, Float, func, event
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy.dialects.mysql import LONGTEXT
from align_data.settings import PINECONE_METADATA_KEYS


logger = logging.getLogger(__name__)


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
    confidence: Mapped[Optional[float]] # Describes the confidence in how good this article is, as a value <0, 1>
    date_published: Mapped[Optional[datetime]]
    meta: Mapped[Optional[JSON]] = mapped_column(JSON, name='metadata', default='{}')
    date_created: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    date_updated: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=func.current_timestamp())
    
    pinecone_update_required: Mapped[bool] = mapped_column(Boolean, default=False)
    pinecone_delete_required: Mapped[bool] = mapped_column(Boolean, default=False)
    incomplete: Mapped[bool] = mapped_column(Boolean, default=False)
    
    summaries: Mapped[List[Summary]] = relationship("Summary", back_populates="article", cascade="all, delete-orphan")

    __id_fields = ['url', 'title']

    def __init__(self, *args, id_fields=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__id_fields = id_fields or self.__id_fields
        if all(getattr(self, field, None) for field in self.__id_fields):
            self.verify_fields()
            id_string = self.generate_id_string()
            self.id = hashlib.md5(id_string).hexdigest()
        else:
            raise ValueError(f"Entry is missing the following field(s): {','.join([missing for missing in self.__id_fields if not getattr(self, missing)])}")

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

    def is_metadata_keys_equal(self, other):
        if not isinstance(other, Article):
            return NotImplemented
        return not any(
            key != 'entry_id' and getattr(self, key, None) != getattr(other, key, None) 
            for key in PINECONE_METADATA_KEYS
        )


    def generate_id_string(self) -> str:
        return ''.join(str(getattr(self, field)) for field in self.__id_fields).encode("utf-8")

    def verify_fields(self):
        missing = [field for field in self.__id_fields if not getattr(self, field)]
        assert not missing, f'Entry is missing the following fields: {missing}'
    
    def verify_id(self):
        assert self.id is not None, "Entry is missing id"

        id_string = self.generate_id_string()
        id_from_fields = hashlib.md5(id_string).hexdigest()
        assert self.id == id_from_fields, f"Entry id {self.id} does not match id from id_fields, {id_from_fields}"

    @classmethod
    def before_write(cls, mapper, connection, target):
        target.verify_fields()

        if target.id:
            target.verify_id()
        else:
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
