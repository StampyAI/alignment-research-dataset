import json
import pytz
import hashlib
from datetime import datetime
from typing import List, Optional
from sqlalchemy import JSON, DateTime, ForeignKey, String, Boolean, Text, Float, func, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.mysql import LONGTEXT


class Base(DeclarativeBase):
    pass


class Summary(Base):

    __tablename__ = "summaries"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(256))
    article_id: Mapped[str] = mapped_column(ForeignKey("articles.id"))

    article: Mapped["Article"] = relationship(back_populates="summaries")


class Pinecone(Base):

    __tablename__ = "pinecone"

    id: Mapped[int] = mapped_column(primary_key=True)
    article_id: Mapped[str] = mapped_column(ForeignKey("articles.id"))
    update_required: Mapped[bool] = mapped_column(Boolean, default=False)
    delete_required: Mapped[bool] = mapped_column(Boolean, default=False)

    article: Mapped["Article"] = relationship("Article", back_populates="pinecone")


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
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # Describes the confidence in how good this article is, as a value <0, 1>
    date_published: Mapped[Optional[datetime]]
    meta: Mapped[Optional[JSON]] = mapped_column(JSON, name='metadata', default='{}')
    date_created: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    date_updated: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=func.current_timestamp())

    summaries: Mapped[List["Summary"]] = relationship(back_populates="article", cascade="all, delete-orphan")
    pinecone: Mapped["Pinecone"] = relationship("Pinecone", uselist=False, back_populates="article")

    __id_fields = ['title', 'url']

    def __init__(self, *args, id_fields, **kwargs):
        self.__id_fields = id_fields
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.title!r}, fullname={self.url!r})"

    def generate_id_string(self):
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
