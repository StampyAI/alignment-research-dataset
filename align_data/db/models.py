import json
import logging
import pytz
import hashlib
from datetime import datetime
from typing import List, Optional
from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    String,
    Boolean,
    Text,
    func,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
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

    _id: Mapped[int] = mapped_column("id", primary_key=True)
    id: Mapped[str] = mapped_column("hash_id", String(32), unique=True, nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(1028))
    url: Mapped[Optional[str]] = mapped_column(String(1028))
    source: Mapped[Optional[str]] = mapped_column(String(128))
    source_type: Mapped[Optional[str]] = mapped_column(String(128))
    authors: Mapped[str] = mapped_column(String(1024))
    text: Mapped[Optional[str]] = mapped_column(LONGTEXT)
    confidence: Mapped[
        Optional[float]
    ]  # Describes the confidence in how good this article is, as a value <0, 1>
    date_published: Mapped[Optional[datetime]]
    meta: Mapped[Optional[JSON]] = mapped_column(JSON, name="metadata", default="{}")
    date_created: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    date_updated: Mapped[Optional[datetime]] = mapped_column(
        DateTime, onupdate=func.current_timestamp()
    )
    status: Mapped[Optional[str]] = mapped_column(String(256))

    pinecone_update_required: Mapped[bool] = mapped_column(Boolean, default=False)

    summaries: Mapped[List["Summary"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )

    __id_fields = ["url", "title"]

    def __init__(self, *args, id_fields, **kwargs):
        self.__id_fields = id_fields
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Article(id={self.id!r}, title={self.title!r}, url={self.url!r}, source={self.source!r}, authors={self.authors!r}, date_published={self.date_published!r})"

    def is_metadata_keys_equal(self, other):
        if not isinstance(other, Article):
            raise TypeError(
                f"Expected an instance of Article, got {type(other).__name__}"
            )
        return not any(
            getattr(self, key, None)
            != getattr(other, key, None)  # entry_id is implicitly ignored
            for key in PINECONE_METADATA_KEYS
        )

    def generate_id_string(self) -> str:
        return "".join(str(getattr(self, field)) for field in self.__id_fields).encode(
            "utf-8"
        )

    @property
    def missing_fields(self):
        return [field for field in self.__id_fields if not getattr(self, field)]

    def verify_fields(self):
        missing = self.missing_fields
        assert not missing, f"Entry is missing the following fields: {missing}"

    def verify_id(self):
        assert self.id is not None, "Entry is missing id"

        id_string = self.generate_id_string()
        id_from_fields = hashlib.md5(id_string).hexdigest()
        assert (
            self.id == id_from_fields
        ), f"Entry id {self.id} does not match id from id_fields, {id_from_fields}"

    def update(self, other):
        for field in self.__table__.columns.keys():
            if field not in ["id", "hash_id", "metadata"] and getattr(other, field):
                setattr(self, field, getattr(other, field))
        self.meta = dict((self.meta or {}), **{k: v for k, v in other.meta.items() if k and v})

        if other._id:
            self._id = other._id
        self.id = None  # update the hash id so it calculates a new one if needed
        return self

    def _set_id(self):
        id_string = self.generate_id_string()
        self.id = hashlib.md5(id_string).hexdigest()

    @classmethod
    def before_write(cls, mapper, connection, target):
        if not target.status and target.missing_fields:
            target.status = f'missing fields: {", ".join(target.missing_fields)}'

        if target.id:
            target.verify_id()
        else:
            target._set_id()

        # This assumes that status pretty much just notes down that an entry is invalid. If it has
        # all fields set and is being written to the database, then it must have been modified, ergo
        # should be also updated in pinecone
        if not target.status:
            target.pinecone_update_required = True

    def to_dict(self):
        if date := self.date_published:
            date = date.replace(tzinfo=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta = json.loads(self.meta) if isinstance(self.meta, str) else self.meta

        authors = []
        if self.authors and self.authors.strip():
            authors = [i.strip() for i in self.authors.split(",")]

        return {
            "id": self.id,
            "title": self.title,
            "url": self.url,
            "source": self.source,
            "source_type": self.source_type,
            "text": self.text,
            "date_published": date,
            "authors": authors,
            "summaries": [s.text for s in (self.summaries or [])],
            **(meta or {}),
        }


event.listen(Article, "before_insert", Article.before_write)
event.listen(Article, "before_update", Article.before_write)
