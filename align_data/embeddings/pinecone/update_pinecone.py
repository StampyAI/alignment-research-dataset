import json
import logging
import time
import traceback
from itertools import islice
from typing import Any, Callable, Iterable, List, Tuple, Generator, Iterator

from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import OperationalError
from tqdm import tqdm
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
        """Helper method to handle update logic with consistent logging.

        Uses ID-based batching to avoid long-running queries.

        WHY NOT STREAMING:
        mysql-connector-python doesn't support server-side cursors
        (supports_server_side_cursors=False), so stream_results=True is ignored.
        Loading 20K articles with TEXT columns takes 100+ minutes and the
        connection dies after ~15 min due to network timeouts.

        FIX: Fetch just IDs first (fast), then load articles in small batches.
        Each batch is a fresh query that completes in seconds.
        """
        # Step 1: Fetch just the IDs (fast - no TEXT columns)
        t0 = time.time()
        article_ids = [row[0] for row in articles_query.with_entities(Article.id).all()]
        logger.debug("Got %d article IDs in %.1fs", len(article_ids), time.time() - t0)

        if log_progress:
            logger.info("Processing %s items", len(article_ids))

        if not article_ids:
            return

        # Step 2: Process in batches, loading full articles for each batch
        num_batches = (len(article_ids) + self.batch_size - 1) // self.batch_size
        batch_iter = range(0, len(article_ids), self.batch_size)

        if log_progress:
            batch_iter = tqdm(
                batch_iter,
                total=num_batches,
                desc="Pinecone update",
                unit="batch",
                dynamic_ncols=True,
            )

        total_processed = 0
        for batch_start in batch_iter:
            batch_ids = article_ids[batch_start:batch_start + self.batch_size]

            # Load full articles for this batch only, eagerly loading summaries
            # to avoid lazy-load queries later that could fail on stale connections
            t0 = time.time()
            articles = session.query(Article).options(
                joinedload(Article.summaries)
            ).filter(Article.id.in_(batch_ids)).all()
            logger.debug("Loaded %d articles in %.1fs", len(articles), time.time() - t0)

            # Flatten all batches from batch_entries into a single list.
            # Normally yields one batch since outer loop already batches by batch_size,
            # but collect all to avoid silent data loss if sizes ever diverge.
            t0 = time.time()
            embedded_batch = []
            for sub_batch in self.batch_entries(iter(articles)):
                embedded_batch.extend(sub_batch)
            logger.debug("Embedded in %.1fs", time.time() - t0)

            t0 = time.time()
            self.save_batch(session, embedded_batch)
            logger.debug("Saved in %.1fs", time.time() - t0)

            total_processed += len(articles)
            if log_progress and hasattr(batch_iter, 'set_postfix'):
                batch_iter.set_postfix(articles=total_processed)

        if log_progress:
            logger.info("Completed processing %s items", total_processed)

    def update(
        self,
        custom_sources: List[str],
        force_update: bool = False,
        log_progress: bool = True,
        only_hashes_from: str = None,
    ):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        :param log_progress: Whether to log progress updates.
        :param only_hashes_from: Path to JSON file containing list of hash_ids to process exclusively (e.g., to_delete.json)
        """
        only_hashes = set()
        if only_hashes_from:
            with open(only_hashes_from) as f:
                only_hashes = set(json.load(f))
            if log_progress:
                logger.info(f"Loaded {len(only_hashes)} hash_ids to process exclusively from {only_hashes_from}")

        with make_session() as session:
            articles_to_update = self._articles_by_source(
                session, custom_sources, force_update
            )

            # Filter to only articles in the specified list
            if only_hashes:
                articles_to_update = articles_to_update.filter(Article.id.in_(only_hashes))
                if log_progress:
                    logger.info(f"Processing only {len(only_hashes)} hash_ids from filter list")

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
        """
        Save a batch to database with retry on MySQL connection loss.

        FIXES ERROR: "Lost connection to MySQL server during query"
        This happens when MySQL connection times out during long embedding operations
        (Voyage API calls can take 30+ seconds per batch).

        WHY THIS FIX WORKS:
        1. SQLAlchemy is configured with pool_pre_ping=True (see align_data/db/session.py:14)
        2. pool_pre_ping means SQLAlchemy pings the connection before use
        3. If ping fails (dead connection), pool automatically provides fresh connection
        4. So after rollback(), the next database operation gets a working connection

        WHY NOT session.close():
        - close() would detach Article objects from the session
        - On retry, process_batch() returns the same Article objects
        - session.add_all() would then try to attach detached objects → state corruption
        - rollback() keeps objects attached but discards pending changes (what we want)

        IDEMPOTENCY:
        - Pinecone upserts are idempotent (same ID = replace)
        - article.pinecone_status = PineconeStatus.added is idempotent (same value)
        - So calling process_batch() multiple times with same batch is safe

        RECOVERY ON FAILURE:
        - If all retries fail, batch stays in pending_addition status
        - Next run will re-query these articles and process them
        """
        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                # process_batch does: Pinecone upsert (idempotent) → modify Article objects
                # The expensive embedding work already happened in _embed_batch()
                processed = self.process_batch(batch)
                session.add_all(processed)
                session.commit()
                return  # Success

            except OperationalError as e:
                # OperationalError includes "Lost connection to MySQL server"
                # and other connection-level failures
                last_error = e
                logger.warning(
                    f"MySQL connection lost (attempt {attempt + 1}/{max_retries}): {e}"
                )

                # Rollback discards pending changes and marks transaction as inactive.
                # pool_pre_ping=True ensures next DB operation gets a live connection.
                # DO NOT call session.close() - see docstring above.
                try:
                    session.rollback()
                except OperationalError:
                    # Rollback itself failed because connection is truly dead.
                    # That's expected - the rollback was best-effort cleanup.
                    # pool_pre_ping will still give us a fresh connection.
                    logger.debug("Rollback failed due to dead connection (expected)")
                except Exception as rollback_err:
                    logger.warning(f"Unexpected error during rollback: {rollback_err}")

                if attempt < max_retries - 1:
                    time.sleep(2 ** (attempt + 1))  # 2s, 4s backoff
                    continue

            except Exception as e:
                # Non-connection errors (validation, Pinecone permanent failure, etc.)
                # Don't retry - these won't resolve with time
                logger.error(f"Batch processing error: {e}")
                traceback.print_exc()
                try:
                    session.rollback()
                except Exception:
                    pass
                return  # Continue to next batch, don't crash entire job

        # All retries exhausted - batch stays in pending_addition status
        # Next run will re-process (just DB write + Pinecone upsert)
        logger.error(
            f"MySQL connection failed after {max_retries} attempts. "
            f"Batch will be reprocessed on next run. Error: {last_error}"
        )

    def batch_entries(
        self, article_stream: Generator[Article, None, None], log_progress: bool = True
    ) -> Iterator[List[Article]]:
        items = iter(article_stream)
        while batch := tuple(islice(items, self.batch_size)):
            yield list(batch)


class PineconeAdder(PineconeAction):
    batch_size = 10

    def __init__(self, *args, skip_status_update=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip_status_update = skip_status_update

    def _articles_by_source(self, session, sources: List[str], force_update: bool):
        return get_pinecone_articles_by_sources(session, sources, force_update)

    def _articles_by_id(self, session, ids: List[str], force_update: bool):
        return get_pinecone_articles_by_ids(session, ids, force_update)

    def process_batch(self, batch: List[Tuple[Article, PineconeEntry | None]]):
        logger.info("Processing batch of %s items", len(batch))
        for article, pinecone_entry in batch:
            if pinecone_entry:
                self.pinecone_db.upsert_entry(pinecone_entry)
                if not self.skip_status_update:
                    article.pinecone_status = PineconeStatus.added
            # If pinecone_entry is None, leave status as pending_addition for retry
        return [a for a, _ in batch]

    def batch_entries(
        self, article_stream: Generator[Article, None, None], log_progress: bool = True
    ) -> Iterator[List[Tuple[Article, PineconeEntry | None]]]:
        """Batch articles and embed them together using voyage-context-3."""
        items = iter(article_stream)
        while True:
            batch = list(islice(items, self.batch_size))
            if not batch:
                break
            t0 = time.time()
            result = self._embed_batch(batch)
            logger.debug("Embedded %d articles in %.1fs", len(batch), time.time() - t0)
            yield result

    def _embed_batch(
        self, articles: List[Article]
    ) -> List[Tuple[Article, PineconeEntry | None]]:
        """Embed a batch of articles together using contextualized embeddings."""
        logger.info("Embedding batch of %s articles", len(articles))

        # Build {article: chunks} for articles with content (now returns Chunk objects)
        t0 = time.time()
        chunks_by_article = {a: get_raw_chunks(a) for a in articles}
        valid_articles = [a for a in articles if chunks_by_article[a]]
        total_chunks = sum(len(chunks_by_article[a]) for a in valid_articles)
        logger.debug("Chunked %d articles (%d chunks) in %.1fs", len(valid_articles), total_chunks, time.time() - t0)
        for a in articles:
            if not chunks_by_article[a]:
                logger.warning(f"No chunks for {a.title}")

        if not valid_articles:
            return [(a, None) for a in articles]

        try:
            # Extract text strings for embedding API (Chunk.value)
            texts_by_article = [[c.value for c in chunks_by_article[a]] for a in valid_articles]
            t0 = time.time()
            all_embeddings = embed_documents_contextualized(texts_by_article)
            logger.debug("Voyage API returned in %.1fs", time.time() - t0)
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
    def __init__(self, *args, skip_status_update=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.adder = PineconeAdder(*args, pinecone=self.pinecone_db, skip_status_update=skip_status_update, **kwargs)
        self.remover = PineconeDeleter(*args, pinecone=self.pinecone_db, **kwargs)

    def update(
        self,
        custom_sources: List[str],
        force_update: bool = False,
        log_progress: bool = True,
        only_hashes_from: str = None,
    ):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        :param log_progress: Whether to log progress updates.
        :param only_hashes_from: Path to JSON file containing list of hash_ids to process exclusively (e.g., to_delete.json)
        """

        if log_progress:
            logger.info("Adding pinecone entries for %s", custom_sources)
        self.adder.update(custom_sources, force_update, log_progress, only_hashes_from=only_hashes_from)

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
