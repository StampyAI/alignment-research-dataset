from datetime import datetime
import logging
import numpy as np
from itertools import islice
from typing import Callable, List, Tuple, Generator

from pydantic import BaseModel, ValidationError, validator

from align_data.common.utils import get_embeddings
from align_data.db.models import Article
from align_data.db.session import make_session, stream_pinecone_updates
from align_data.pinecone.pinecone_db_handler import PineconeDB
from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter


logger = logging.getLogger(__name__)


# Define type aliases for the Callables
LengthFunctionType = Callable[[str], int]
TruncateFunctionType = Callable[[str, int], str]


class PineconeEntry(BaseModel):
    id: str
    source: str
    title: str
    url: str
    date_published: datetime
    authors: List[str]
    text_chunks: List[str]
    embeddings: np.ndarray

    class Config:
        arbitrary_types_allowed = True

    def __repr__(self):
        return f"PineconeEntry(id={self.id!r}, source={self.source!r}, title={self.title!r}, url={self.url!r}, date_published={self.date_published!r}, authors={self.authors!r}, text_chunks={self.text_chunks[:5]!r})"

    @validator(
        "id",
        "source",
        "title",
        "url",
        "date_published",
        "authors",
        "text_chunks",
        pre=True,
        always=True,
    )
    def empty_strings_not_allowed(cls, value):
        if not str(value).strip():
            raise ValueError("Attribute should not be empty.")
        return value


class PineconeUpdater:
    def __init__(self):
        self.text_splitter = ParagraphSentenceUnitTextSplitter()
        self.pinecone_db = PineconeDB()

    def update(self, custom_sources: List[str], force_update: bool = False):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        """
        with make_session() as session:
            entries_stream = stream_pinecone_updates(session, custom_sources, force_update)
            for batch in self.batch_entries(entries_stream):
                self.save_batch(session, batch)

    def save_batch(self, session, batch):
        try:
            for article, pinecone_entry in batch:
                self.pinecone_db.upsert_entry(pinecone_entry.dict())
                article.pinecone_update_required = False
                session.add(article)
            session.commit()
        except Exception as e:
            # Rollback on any kind of error. The next run will redo this batch, but in the meantime keep trucking
            logger.error(e)
            session.rollback()

    def batch_entries(
        self, article_stream: Generator[Article, None, None]
    ) -> Generator[List[Tuple[Article, PineconeEntry]], None, None]:
        items = iter(article_stream)
        while batch := tuple(islice(items, 10)):
            yield list(filter(None, map(self._make_pinecone_update, batch)))

    def _make_pinecone_update(self, article: Article):
        try:
            text_chunks = get_text_chunks(article, self.text_splitter)
            return article, PineconeEntry(
                id=article.id,
                source=article.source,
                title=article.title,
                url=article.url,
                date_published=article.date_published,
                authors=[
                    author.strip()
                    for author in article.authors.split(",")
                    if author.strip()
                ],
                text_chunks=text_chunks,
                embeddings=get_embeddings(text_chunks, article.source)
            )
        except (ValueError, ValidationError) as e:
            logger.exception(e)


def get_text_chunks(article: Article, text_splitter: ParagraphSentenceUnitTextSplitter) -> List[str]:
    title = article.title.replace("\n", " ")
    
    authors_lst = [author.strip() for author in article.authors.split(",")]
    authors = get_authors_str(authors_lst)
    
    signature = f"Title: {title}; Author(s): {authors}."
    text_chunks = text_splitter.split_text(article.text)
    return [f"{signature}\n\"{text_chunk}\"" for text_chunk in text_chunks]

def get_authors_str(authors_lst: List[str]) -> str:
    if authors_lst == []:
        return "n/a"
    if len(authors_lst) == 1:
        return authors_lst[0]
    else:
        authors_lst = authors_lst[:4]
        authors_str = f"{', '.join(authors_lst[:-1])} and {authors_lst[-1]}"
    return authors_str.replace("\n", " ")
