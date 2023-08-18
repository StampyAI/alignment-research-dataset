from datetime import datetime
import logging
import numpy as np
import os
from typing import Callable, List, Tuple, Generator

import openai
from pydantic import BaseModel, ValidationError, validator

from align_data.common.utils import get_embeddings
from align_data.db.models import Article
from align_data.db.session import make_session, stream_pinecone_updates
from align_data.pinecone.pinecone_db_handler import PineconeDB
from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter
from align_data.settings import MAX_NUM_AUTHORS_IN_SIGNATURE


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

    def update(self, custom_sources: List[str]):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        """
        with make_session() as session:
            entries_stream = stream_pinecone_updates(session, custom_sources)
            for article, pinecone_entry in self.process_entries(entries_stream):
                self.pinecone_db.upsert_entry(pinecone_entry.dict())
                article.pinecone_update_required = False
                session.add(article)
            session.commit()

    def process_entries(
        self, article_stream: Generator[Article, None, None]
    ) -> Generator[Tuple[Article, PineconeEntry], None, None]:
        for article in article_stream:
            try:
                text_chunks = get_text_chunks(article, self.text_splitter)
                yield article, PineconeEntry(
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
                    embeddings=get_embeddings(text_chunks, article.source),
                )
            except (ValueError, ValidationError) as e:
                logger.exception(e)


def get_text_chunks(article: Article, text_splitter: ParagraphSentenceUnitTextSplitter) -> List[str]:
    if isinstance(article.authors, str):
        authors_lst = [author.strip() for author in article.authors.split(",")]
        authors = get_authors_str(authors_lst)
    elif isinstance(article.authors, list):
        authors = get_authors_str(article.authors)
    
    signature = f"Title: {article.title}, Author(s): {authors}"
    if not isinstance(article.text, str):
        raise ValueError(f"Article text is not a string: {article.text}")
    text_chunks = text_splitter.split_text(article.text)
    text_chunks = [f"- {signature}\n\n{text_chunk}" for text_chunk in text_chunks]
    return text_chunks


def get_authors_str(authors_lst: List[str]) -> str:
    if authors_lst == []:
        return "n/a"
    if len(authors_lst) == 1:
        return authors_lst[0]
    else:
        authors_lst = authors_lst[:MAX_NUM_AUTHORS_IN_SIGNATURE]
        authors_str = f"{', '.join(authors_lst[:-1])} and {authors_lst[-1]}"
    return authors_str
