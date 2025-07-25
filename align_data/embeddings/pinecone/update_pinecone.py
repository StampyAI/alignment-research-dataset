import logging
import traceback
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
)
from align_data.embeddings.text_splitter import split_text

logger = logging.getLogger(__name__)

# Define type aliases for the Callables
LengthFunctionType = Callable[[str], int]
TruncateFunctionType = Callable[[str, int], str]


class PineconeAction:
    batch_size = 10

    def __init__(self, pinecone=None):
        self.pinecone_db = pinecone or PineconeDB()

    def _articles_by_source(
        self, session: Session, sources: List[str], force_update: bool
    ) -> Iterable[Article]:
        raise NotImplementedError

    def _articles_by_id(
        self, session: Session, ids: List[str], force_update: bool
    ) -> Iterable[Article]:
        raise NotImplementedError

    def _update_with_logging(
        self, session: Session, articles_query, log_progress: bool
    ):
        """Helper method to handle update logic with consistent logging."""
        total_articles = articles_query.count()

        if log_progress:
            logger.info("Processing %s items", total_articles)

        total_processed = 0
        for batch in self.batch_entries(articles_query):
            self.save_batch(session, batch)
            total_processed += len(batch)

            if log_progress:
                percentage = (
                    (total_processed / total_articles) * 100
                    if total_articles > 0
                    else 0
                )
                logger.info(
                    "Progress: %.1f%% (%d/%d)",
                    percentage,
                    total_processed,
                    total_articles,
                )

        if log_progress:
            logger.info("Completed processing %s items", total_processed)

    def update(
        self,
        custom_sources: List[str],
        force_update: bool = False,
        log_progress: bool = True,
    ):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        :param log_progress: Whether to log progress updates.
        """
        with make_session() as session:
            articles_to_update = self._articles_by_source(
                session, custom_sources, force_update
            )
            self._update_with_logging(session, articles_to_update, log_progress)

    def update_articles_by_ids(
        self, hash_ids: List[int], force_update: bool = False, log_progress: bool = True
    ):
        """Update the Pinecone entries of specific articles based on their hash_ids."""
        with make_session() as session:
            articles_to_update = self._articles_by_id(session, hash_ids, force_update)
            self._update_with_logging(session, articles_to_update, log_progress)

    def process_batch(
        self, batch: List[Tuple[Article, PineconeEntry | None]]
    ) -> List[Article]:
        raise NotImplementedError

    def save_batch(self, session: Session, batch: List[Any]):
        try:
            session.add_all(self.process_batch(batch))
            session.commit()

        except Exception as e:
            # Rollback on any kind of error. The next run will redo this batch, but in the meantime keep trucking
            logger.error(e)
            traceback.print_exc()
            session.rollback()

    def batch_entries(
        self, article_stream: Generator[Article, None, None], log_progress: bool = True
    ) -> Iterator[List[Article]]:
        items = iter(article_stream)
        while batch := tuple(islice(items, self.batch_size)):
            yield list(batch)


class PineconeAdder(PineconeAction):
    batch_size = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _articles_by_source(self, session, sources: List[str], force_update: bool):
        return get_pinecone_articles_by_sources(session, sources, force_update)

    def _articles_by_id(self, session, ids: List[str], force_update: bool):
        return get_pinecone_articles_by_ids(session, ids, force_update)

    def process_batch(self, batch: List[Tuple[Article, PineconeEntry | None]]):
        logger.info("Processing batch of %s items", len(batch))
        for article, pinecone_entry in batch:
            if pinecone_entry:
                self.pinecone_db.upsert_entry(pinecone_entry)

            article.pinecone_status = PineconeStatus.added
        return [a for a, _ in batch]

    def batch_entries(
        self, article_stream: Generator[Article, None, None], log_progress: bool = True
    ) -> Iterator[List[Tuple[Article, PineconeEntry | None]]]:
        items = iter(article_stream)
        while batch := tuple(islice(items, self.batch_size)):
            yield [(article, self._make_pinecone_entry(article)) for article in batch]

    def _make_pinecone_entry(self, article: Article) -> PineconeEntry | None:
        logger.info(f"Getting embeddings for {article.title}")
        article.comments = ""
        try:
            text_chunks = get_text_chunks(article)
            embeddings, moderation_results = get_embeddings(text_chunks)
            if not embeddings:
                logger.warning(f"No embeddings found for {article.title}")
                logger.warning(f"Moderation results: {moderation_results}")
                logger.info("text: %s", text_chunks)
                return None

            if moderation_results:
                flagged_text_chunks = [result["text"] for result in moderation_results]
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
                authors=[
                    author.strip()
                    for author in article.authors.split(",")
                    if author.strip()
                ],
                embeddings=embeddings,
                confidence=article.confidence,
                miri_confidence=article.miri_confidence,
                miri_distance=article.miri_distance or "general",
                needs_tech=article.needs_tech,
            )
        except (
            # ValueError,
            # TypeError,
            # AttributeError,
            ValidationError,
            # MissingFieldsError,
            # MissingEmbeddingModelError,
        ) as e:
            logger.warning(e)
            article.append_comment(
                f"Error encountered while processing this article: {e}"
            )
            return None

        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            raise


class PineconeDeleter(PineconeAction):
    pinecone_statuses = [PineconeStatus.pending_removal]

    def _articles_by_source(self, session, sources: List[str], _force_update: bool):
        return get_pinecone_to_delete_by_sources(session, sources)

    def _articles_by_id(self, session, ids: List[str], _force_update: bool):
        return get_pinecone_to_delete_by_ids(session, ids)

    def process_batch(self, batch: List[Article]):
        self.pinecone_db.delete_entries([a.id for a in batch])
        logger.info("removing batch of %s items", len(batch))
        for article in batch:
            article.pinecone_status = PineconeStatus.absent
        return batch


class PineconeUpdater(PineconeAction):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adder = PineconeAdder(*args, pinecone=self.pinecone_db, **kwargs)
        self.remover = PineconeDeleter(*args, pinecone=self.pinecone_db, **kwargs)

    def update(
        self,
        custom_sources: List[str],
        force_update: bool = False,
        log_progress: bool = True,
    ):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        :param log_progress: Whether to log progress updates.
        """

        if log_progress:
            logger.info("Adding pinecone entries for %s", custom_sources)
        self.adder.update(custom_sources, force_update, log_progress)

        if log_progress:
            logger.info("Removing outdated pinecone entries for %s", custom_sources)
        self.remover.update(custom_sources, force_update, log_progress)

        if log_progress:
            logger.info("Pinecone update completed")

    def update_articles_by_ids(
        self, hash_ids: List[int], force_update: bool = False, log_progress: bool = True
    ):
        """Update the Pinecone entries of specific articles based on their hash_ids."""
        if log_progress:
            logger.info("Adding pinecone entries by hash_id")
        self.adder.update_articles_by_ids(hash_ids, force_update, log_progress)

        if log_progress:
            logger.info("Removing outdated pinecone entries by hash_id")
        self.remover.update_articles_by_ids(hash_ids, force_update, log_progress)

        if log_progress:
            logger.info("Pinecone update by ID completed")


def get_text_chunks(article: Article) -> List[str]:
    title = article.title.replace("\n", " ")

    authors_lst = [author.strip() for author in article.authors.split(",")]
    authors = get_authors_str(authors_lst)

    signature = f"Title: {title}; Author(s): {authors}."

    text_chunks = split_text(article.text)
    for summary in article.summaries:
        text_chunks += split_text(summary.text)

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
