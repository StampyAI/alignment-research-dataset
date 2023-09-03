import logging
from itertools import islice
from typing import Any, Callable, Iterable, List, Tuple, Generator, Iterator

from sqlalchemy.orm import Session
from pydantic import ValidationError

from align_data.embeddings.embedding_utils import get_embeddings
from align_data.db.models import Article, PineconeStatus
from align_data.db.session import (
    make_session,
    get_pinecone_articles_by_sources,
    get_pinecone_articles_by_ids,
    get_pinecone_to_delete_by_sources,
    get_pinecone_to_delete_by_ids,
)
from align_data.embeddings.pinecone.pinecone_db_handler import PineconeDB
from align_data.embeddings.pinecone.pinecone_models import (
    PineconeEntry,
    MissingFieldsError,
    MissingEmbeddingModelError,
)
from align_data.embeddings.text_splitter import ParagraphSentenceUnitTextSplitter


logger = logging.getLogger(__name__)


# Define type aliases for the Callables
LengthFunctionType = Callable[[str], int]
TruncateFunctionType = Callable[[str, int], str]


class PineconeAction:
    batch_size = 10

    def __init__(self, pinecone=None):
        self.pinecone_db = pinecone or PineconeDB()

    def _articles_by_source(self, session: Session, sources: List[str], force_update: bool) -> Iterable[Article]:
        raise NotImplementedError

    def _articles_by_id(self, session: Session, ids: List[str], force_update: bool) -> Iterable[Article]:
        raise NotImplementedError

    def update(self, custom_sources: List[str], force_update: bool = False):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        """
        with make_session() as session:
            articles_to_update = self._articles_by_source(session, custom_sources, force_update)
            logger.info('Processing %s items', articles_to_update.count())
            for batch in self.batch_entries(articles_to_update):
                self.save_batch(session, batch)

    def update_articles_by_ids(self, hash_ids: List[int], force_update: bool = False):
        """Update the Pinecone entries of specific articles based on their hash_ids."""
        with make_session() as session:
            articles_to_update = self._articles_by_id(session, hash_ids, force_update)
            for batch in self.batch_entries(articles_to_update):
                self.save_batch(session, batch)

    def process_batch(self, batch: List[Tuple[Article, PineconeEntry | None]]) -> List[Article]:
        raise NotImplementedError

    def save_batch(self, session: Session, batch: List[Any]):
        try:
            session.add_all(self.process_batch(batch))
            session.commit()

        except Exception as e:
            # Rollback on any kind of error. The next run will redo this batch, but in the meantime keep trucking
            logger.error(e)
            session.rollback()

    def batch_entries(self, article_stream: Generator[Article, None, None]) -> Iterator[List[Article]]:
        while batch := tuple(islice(article_stream, self.batch_size)):
            yield list(batch)


class PineconeAdder(PineconeAction):
    batch_size = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text_splitter = ParagraphSentenceUnitTextSplitter()

    def _articles_by_source(self, session, sources: List[str], force_update: bool):
        return get_pinecone_articles_by_sources(session, sources, force_update)

    def _articles_by_id(self, session, ids: List[str], force_update: bool):
        return get_pinecone_articles_by_ids(session, ids, force_update)

    def process_batch(self, batch: List[Tuple[Article, PineconeEntry | None]]):
        for article, pinecone_entry in batch:
            if pinecone_entry:
                self.pinecone_db.upsert_entry(pinecone_entry)

            article.pinecone_status = PineconeStatus.added
            return [a for a, _ in batch]

    def batch_entries(
        self, article_stream: Generator[Article, None, None]
    ) -> Iterator[List[Tuple[Article, PineconeEntry | None]]]:
        while batch := tuple(islice(article_stream, self.batch_size)):
            yield [(article, self._make_pinecone_entry(article)) for article in batch]

    def _make_pinecone_entry(self, article: Article) -> PineconeEntry | None:
        try:
            text_chunks = get_text_chunks(article, self.text_splitter)
            embeddings, moderation_results = get_embeddings(text_chunks, article.source)

            if any(result["flagged"] for result in moderation_results):
                flagged_text_chunks = [
                    f'Chunk {i}: "{text}"'
                    for i, (text, result) in enumerate(zip(text_chunks, moderation_results))
                    if result["flagged"]
                ]
                logger.warning(
                    f"OpenAI moderation flagged text chunks for the following article: {article.id}"
                )
                article.append_comment(
                    f"OpenAI moderation flagged the following text chunks: {flagged_text_chunks}"
                )

            return PineconeEntry(
                hash_id=article.id,  # the hash_id of the article
                source=article.source,
                title=article.title,
                url=article.url,
                date_published=article.date_published.timestamp(),
                authors=[author.strip() for author in article.authors.split(",") if author.strip()],
                text_chunks=text_chunks,
                embeddings=embeddings,
                confidence=article.confidence,
            )
        except (
            ValueError,
            TypeError,
            AttributeError,
            ValidationError,
            MissingFieldsError,
            MissingEmbeddingModelError,
        ) as e:
            logger.warning(e)
            article.append_comment(f"Error encountered while processing this article: {e}")
            return None

        except Exception as e:
            logger.error(e)
            raise


class PineconeDeleter(PineconeAction):
    batch_size = 100
    pinecone_statuses = [PineconeStatus.pending_removal]

    def _articles_by_source(self, session, sources: List[str], _force_update: bool):
        return get_pinecone_to_delete_by_sources(session, sources)

    def _articles_by_id(self, session, ids: List[str], _force_update: bool):
        return get_pinecone_to_delete_by_ids(session, ids)

    def process_batch(self, batch: List[Article]):
        self.pinecone_db.delete_entries([a.id for a in batch])
        logger.info('removing batch %s', len(batch))
        for article in batch:
            article.pinecone_status = PineconeStatus.removed
        return batch


class PineconeUpdater(PineconeAction):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adder = PineconeAdder(*args, pinecone=self.pinecone_db, **kwargs)
        self.remover = PineconeDeleter(*args, pinecone=self.pinecone_db, **kwargs)

    def update(self, custom_sources: List[str], force_update: bool = False):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        """
        logger.info('Adding pinecone entries for %s', custom_sources)
        self.adder.update(custom_sources, force_update)

        logger.info('Removing pinecone entries for %s', custom_sources)
        self.remover.update(custom_sources, force_update)

    def update_articles_by_ids(self, hash_ids: List[int], force_update: bool = False):
        """Update the Pinecone entries of specific articles based on their hash_ids."""
        logger.info('Adding pinecone entries by hash_id')
        self.adder.update_articles_by_ids(hash_ids, force_update)
        logger.info('Removing pinecone entries by hash_id')
        self.remover.update_articles_by_ids(hash_ids, force_update)


def get_text_chunks(
    article: Article, text_splitter: ParagraphSentenceUnitTextSplitter
) -> List[str]:
    title = article.title.replace("\n", " ")

    authors_lst = [author.strip() for author in article.authors.split(",")]
    authors = get_authors_str(authors_lst)

    signature = f"Title: {title}; Author(s): {authors}."

    text_chunks = text_splitter.split_text(article.text)
    for summary in article.summaries:
        text_chunks += text_splitter.split_text(summary.text)

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
