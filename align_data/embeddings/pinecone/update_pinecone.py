import logging
import traceback
from itertools import islice
from typing import Any, Callable, Iterable, List, Tuple, Generator, Iterator

from sqlalchemy.orm import Session
from pydantic import ValidationError

from align_data.embeddings.embedding_utils import (
    embed_documents_contextualized,
    Embedding,
)
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
from align_data.embeddings.text_splitter import split_text, Chunk

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
            # If pinecone_entry is None, leave status as pending_addition for retry
        return [a for a, _ in batch]

    def batch_entries(
        self, article_stream: Generator[Article, None, None], log_progress: bool = True
    ) -> Iterator[List[Tuple[Article, PineconeEntry | None]]]:
        """Batch articles and embed them together using voyage-context-3."""
        items = iter(article_stream)
        while batch := list(islice(items, self.batch_size)):
            yield self._embed_batch(batch)

    def _embed_batch(
        self, articles: List[Article]
    ) -> List[Tuple[Article, PineconeEntry | None]]:
        """Embed a batch of articles together using contextualized embeddings."""
        logger.info("Embedding batch of %s articles", len(articles))

        # Build {article: chunks} for articles with content (now returns Chunk objects)
        chunks_by_article = {a: get_raw_chunks(a) for a in articles}
        valid_articles = [a for a in articles if chunks_by_article[a]]
        for a in articles:
            if not chunks_by_article[a]:
                logger.warning(f"No chunks for {a.title}")

        if not valid_articles:
            return [(a, None) for a in articles]

        try:
            # Extract text strings for embedding API (Chunk.value)
            texts_by_article = [[c.value for c in chunks_by_article[a]] for a in valid_articles]
            all_embeddings = embed_documents_contextualized(texts_by_article)
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Batch embedding failed: {e}")
            for a in valid_articles:
                a.append_comment(f"Batch embedding error: {e}")
            return [(a, None) for a in articles]

        # Map embeddings back to articles
        embeddings_by_article = dict(zip(valid_articles, all_embeddings))
        results = []
        for article in articles:
            vectors = embeddings_by_article.get(article, [])
            if not vectors:
                results.append((article, None))
                continue
            chunks = chunks_by_article[article]
            assert len(vectors) == len(chunks), f"Embedding count mismatch: {len(vectors)} vs {len(chunks)}"
            # Create Embeddings with section_heading from Chunks
            embeddings = [
                Embedding(vector=v, text=c.value, section_heading=c.section_heading)
                for v, c in zip(vectors, chunks)
            ]
            results.append((article, self._make_pinecone_entry_from_embeddings(article, embeddings)))
        return results

    def _make_pinecone_entry_from_embeddings(
        self, article: Article, embeddings: List[Embedding]
    ) -> PineconeEntry | None:
        """Create a PineconeEntry from pre-computed embeddings."""
        # Extract meta fields (GreaterWrong articles have tags, karma, etc.)
        meta = article.meta if isinstance(article.meta, dict) else {}
        tags = [t.strip() for t in meta.get('tags', []) if t and t.strip()]

        try:
            return PineconeEntry(
                hash_id=article.id,
                source=article.source,
                title=article.title,
                url=article.url,
                date_published=article.date_published.timestamp() if article.date_published else None,
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
                tags=tags,
                karma=meta.get('karma'),
                votes=meta.get('votes'),
                comment_count=meta.get('comment_count'),
                source_type=article.source_type,
            )
        except ValidationError as e:
            logger.warning(e)
            article.append_comment(f"Error creating PineconeEntry: {e}")
            return None


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


def get_raw_chunks(article: Article) -> List[Chunk]:
    """Get raw text chunks with metadata for an article.

    For voyage-context-3, chunks are embedded with awareness of their siblings,
    so we don't need to repeat title/author in each chunk.
    Returns full Chunk objects with section heading metadata.
    """
    text_chunks = split_text(article.text)
    for summary in article.summaries:
        text_chunks += split_text(summary.text)
    return [c for c in text_chunks if c.value and c.value.strip()]
