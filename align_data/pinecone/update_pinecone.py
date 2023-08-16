from datetime import datetime
import logging
import numpy as np
import os
from typing import Callable, List, Tuple, Generator

import openai
from pydantic import BaseModel, ValidationError, validator

from align_data.db.models import Article
from align_data.db.session import make_session, stream_pinecone_updates
from align_data.pinecone.pinecone_db_handler import PineconeDB
from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter
from align_data.settings import (
    USE_OPENAI_EMBEDDINGS,
    OPENAI_EMBEDDINGS_MODEL,
    OPENAI_EMBEDDINGS_DIMS,
    OPENAI_EMBEDDINGS_RATE_LIMIT,
    SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
    SENTENCE_TRANSFORMER_EMBEDDINGS_DIMS,
    CHUNK_SIZE,
    MAX_NUM_AUTHORS_IN_SIGNATURE,
    EMBEDDING_LENGTH_BIAS,
)

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
    def __init__(
        self,
        min_chunk_size: int = ParagraphSentenceUnitTextSplitter.DEFAULT_MIN_CHUNK_SIZE,
        max_chunk_size: int = ParagraphSentenceUnitTextSplitter.DEFAULT_MAX_CHUNK_SIZE,
        length_function: LengthFunctionType = ParagraphSentenceUnitTextSplitter.DEFAULT_LENGTH_FUNCTION,
        truncate_function: TruncateFunctionType = ParagraphSentenceUnitTextSplitter.DEFAULT_TRUNCATE_FUNCTION,
    ):
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.length_function = length_function
        self.truncate_function = truncate_function

        self.text_splitter = ParagraphSentenceUnitTextSplitter(
            min_chunk_size=self.min_chunk_size,
            max_chunk_size=self.max_chunk_size,
            length_function=self.length_function,
            truncate_function=self.truncate_function,
        )
        self.pinecone_db = PineconeDB()

        if USE_OPENAI_EMBEDDINGS:
            import openai

            openai.api_key = os.environ["OPENAI_API_KEY"]
        else:
            import torch
            from langchain.embeddings import HuggingFaceEmbeddings

            self.hf_embeddings = HuggingFaceEmbeddings(
                model_name=SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
                model_kwargs={"device": "cuda" if torch.cuda.is_available() else "cpu"},
                encode_kwargs={"show_progress_bar": False},
            )

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
                text_chunks = self.get_text_chunks(article)
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
                    embeddings=self.extract_embeddings(
                        text_chunks, [article.source] * len(text_chunks)
                    ),
                )
            except (ValueError, ValidationError) as e:
                logger.exception(e)

    def get_text_chunks(self, article: Article) -> List[str]:
        signature = f"Title: {article.title}, Author(s): {self.get_authors_str(article.authors)}"
        text_chunks = self.text_splitter.split_text(article.text)
        text_chunks = [f"- {signature}\n\n{text_chunk}" for text_chunk in text_chunks]
        return text_chunks

    def extract_embeddings(self, chunks_batch, sources_batch):
        if USE_OPENAI_EMBEDDINGS:
            return self.get_openai_embeddings(chunks_batch, sources_batch)
        else:
            return np.array(
                self.hf_embeddings.embed_documents(chunks_batch, sources_batch)
            )

    @staticmethod
    def get_openai_embeddings(chunks, sources=""):
        embeddings = np.zeros((len(chunks), OPENAI_EMBEDDINGS_DIMS))

        openai_output = openai.Embedding.create(
            model=OPENAI_EMBEDDINGS_MODEL, input=chunks
        )["data"]

        for i, (embedding, source) in enumerate(zip(openai_output, sources)):
            bias = EMBEDDING_LENGTH_BIAS.get(source, 1.0)
            embeddings[i] = bias * np.array(embedding["embedding"])

        return embeddings

    @staticmethod
    def get_authors_str(authors_lst: List[str]) -> str:
        if authors_lst == []:
            return "n/a"
        if len(authors_lst) == 1:
            return authors_lst[0]
        else:
            authors_lst = authors_lst[:MAX_NUM_AUTHORS_IN_SIGNATURE]
            authors_str = f"{', '.join(authors_lst[:-1])} and {authors_lst[-1]}"
        return authors_str
