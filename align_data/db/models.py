import enum
import re
import json
import re
import logging
import pytz
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    func,
    event,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, validates
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.hybrid import hybrid_property

from align_data.embeddings.pinecone.pinecone_models import PineconeMetadata

logger = logging.getLogger(__name__)
OK_STATUS = None


class Base(DeclarativeBase):
    pass


class Summary(Base):
    __tablename__ = "summaries"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(256))
    article_id: Mapped[str] = mapped_column(ForeignKey("articles.id"))

    article: Mapped["Article"] = relationship(back_populates="summaries")


class PineconeStatus(enum.Enum):
    absent = 1
    pending_removal = 2
    pending_addition = 3
    added = 4


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
    date_checked: Mapped[datetime] = mapped_column(DateTime, default=func.now())  # The timestamp when this article was last checked if still valid
    status: Mapped[Optional[str]] = mapped_column(String(256))
    comments: Mapped[Optional[str]] = mapped_column(LONGTEXT)  # Editor comments. Can be anything

    pinecone_status: Mapped[PineconeStatus] = mapped_column(Enum(PineconeStatus), default=PineconeStatus.absent)

    summaries: Mapped[List["Summary"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"Article(id={self.id!r}, title={self.title!r}, url={self.url!r}, source={self.source!r}, authors={self.authors!r}, date_published={self.date_published!r})"

    def generate_id_string(self) -> bytes:
        return "".join(
            re.sub(r"[^a-zA-Z0-9\s]", "", str(getattr(self, field))).strip().lower()
            for field in self.__id_fields
        ).encode("utf-8")

    @property
    def __id_fields(self) -> List[str]:
        if self.source in ["importai", "ml_safety_newsletter", "alignment_newsletter"]:
            return ["url", "source"]
        return ["url"]

    @property
    def missing_fields(self) -> List[str]:
        fields = set(self.__id_fields) | {
            "text",
            "title",
            "url",
            "source",
            "date_published",
        }
        return sorted([field for field in fields if not getattr(self, field, None)])

    def verify_id(self):
        assert self.id is not None, "Entry is missing id"

        id_string = self.generate_id_string()
        id_from_fields = hashlib.md5(id_string).hexdigest()
        assert (
            self.id == id_from_fields
        ), f"Entry id {self.id} does not match id from id_fields: {id_from_fields}"

    def verify_id_fields(self):
        missing = [field for field in self.__id_fields if not getattr(self, field)]
        assert not missing, f"Entry is missing the following fields: {missing}"

    def update(self, other: "Article") -> "Article":
        for field in self.__table__.columns.keys():
            if field not in ["id", "hash_id", "metadata"]:
                new_value = getattr(other, field)
                if new_value and getattr(self, field) != new_value:
                    setattr(self, field, new_value)

        updated_meta = dict((self.meta or {}), **{k: v for k, v in other.meta.items() if k and v})
        if self.meta != updated_meta:
            self.meta = updated_meta

        if other._id:
            self._id = other._id
        self.id = None  # update the hash id so it calculates a new one if needed
        return self
    
    @validates('text')
    def validate_text(self, key, text):
        if text is None:
            return None
        # Remove or escape problematic characters
        return text.replace("'", "''")

    def _set_id(self):
        id_string = self.generate_id_string()
        self.id = hashlib.md5(id_string).hexdigest()

    def add_meta(self, key: str, val):
        if self.meta is None:
            self.meta = {}
        self.meta[key] = val

    def append_comment(self, comment: str):
        if self.comments is None:
            self.comments = ""
        self.comments = f"{self.comments}\n\n{comment}".strip()

    @hybrid_property
    def is_valid(self) -> bool:
        # Check if the basic attributes are present and non-empty
        basic_check = all(
            [
                self.text and self.text.strip(),
                self.url and self.url.strip(),
                self.title and self.title.strip(),
                self.authors,
                self.status == OK_STATUS,
            ]
        )

        return basic_check

    @is_valid.expression
    def is_valid(cls) -> bool:
        return (
            (cls.status == OK_STATUS)
            & (cls.text != None)
            & (cls.url != None)
            & (cls.title != None)
            & (cls.authors != None)
        )

    @classmethod
    def before_write(cls, _mapper, _connection, target: "Article"):
        target.verify_id_fields()

        if not target.status and target.missing_fields:
            target.status = "Missing fields"
            target.comments = f'missing fields: {", ".join(target.missing_fields)}'

        if target.id:
            target.verify_id()
        else:
            target._set_id()

    @classmethod
    def check_for_changes(cls, mapper, connection, target):
        if not target.is_valid:
            return
        monitored_attributes = list(PineconeMetadata.__annotations__.keys())
        monitored_attributes.remove("hash_id")

        if any(get_history(target, attr).has_changes() for attr in monitored_attributes):
            target.pinecone_status = PineconeStatus.pending_addition

    def to_dict(self) -> Dict[str, Any]:
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
event.listen(Article, "before_insert", Article.check_for_changes)
event.listen(Article, "before_update", Article.check_for_changes)
