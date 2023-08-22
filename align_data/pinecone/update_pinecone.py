from datetime import datetime
import logging
from itertools import islice
from typing import Callable, List, Tuple, Generator, Iterator, Any, Dict

from sqlalchemy.orm import Session
from pydantic import ValidationError

from align_data.common.utils import get_embeddings
from align_data.db.models import Article
from align_data.db.session import make_session, stream_pinecone_updates
from align_data.pinecone.pinecone_db_handler import PineconeDB
from align_data.pinecone.pinecone_models import PineconeEntry
from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter


logger = logging.getLogger(__name__)


# Define type aliases for the Callables
LengthFunctionType = Callable[[str], int]
TruncateFunctionType = Callable[[str, int], str]


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
            articles_to_update_stream = stream_pinecone_updates(session, custom_sources, force_update)
            for batch in self.batch_entries(articles_to_update_stream):
                self.save_batch(session, batch)

    def save_batch(self, session: Session, batch: List[Tuple[Article, PineconeEntry]]):
        try:
            for article, pinecone_entry in batch:
                self.pinecone_db.upsert_entry(pinecone_entry)
                article.pinecone_update_required = False
                session.add(article)
            session.commit()
        except Exception as e:
            # Rollback on any kind of error. The next run will redo this batch, but in the meantime keep trucking
            logger.error(e)
            session.rollback()

    def batch_entries(
        self, article_stream: Generator[Article, None, None]
    ) -> Iterator[List[Tuple[Article, PineconeEntry]]]:
        while batch := tuple(islice(article_stream, 10)):
            yield [
                (article, pinecone_entry) 
                for article in batch 
                if (pinecone_entry := self._make_pinecone_entry(article)) is not None
                ]

    def _make_pinecone_entry(self, article: Article) -> PineconeEntry:
        text_chunks = get_text_chunks(article, self.text_splitter)
        assert isinstance(article.date_published, datetime)
        try:
            return PineconeEntry(
                id=article.id, # the hash_id of the article
                source=article.source,
                title=article.title,
                url=article.url,
                date=article.date_published.timestamp(),
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
    return [f'###{signature}###\n"""{text_chunk}"""' for text_chunk in text_chunks]

def get_authors_str(authors_lst: List[str]) -> str:
    if not authors_lst:
        return "n/a"
    
    if len(authors_lst) == 1:
        authors_str = authors_lst[0]
    else:
        authors_lst = authors_lst[:4]
        authors_str = f"{', '.join(authors_lst[:-1])} and {authors_lst[-1]}"
    
    authors_str = authors_str.replace("\n", " ")

    # Truncate if necessary
    if len(authors_str) > 500:
        authors_str = authors_str[:497] + "..."
    
    return authors_str
