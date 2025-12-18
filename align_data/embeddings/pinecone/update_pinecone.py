import logging
import time
import traceback
from itertools import islice
from typing import Any, Callable, Iterable, List, Tuple, Generator, Iterator

from sqlalchemy.orm import Session
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
        """Helper method to handle update logic with consistent logging."""
        import time
        print(f"[DEBUG] Starting count query...", flush=True)
        t0 = time.time()
        total_articles = articles_query.count()
        print(f"[DEBUG] Count query done: {total_articles} articles in {time.time()-t0:.1f}s", flush=True)

        if log_progress:
            logger.info("Processing %s items", total_articles)

        # Calculate number of batches for tqdm
        num_batches = (total_articles + self.batch_size - 1) // self.batch_size if total_articles > 0 else 0

        # yield_per streams results instead of loading all into memory
        # execution_options with stream_results enables server-side cursor
        print(f"[DEBUG] Setting up streaming query...", flush=True)
        t0 = time.time()
        streaming_query = articles_query.execution_options(stream_results=True).yield_per(100)
        batch_iter = self.batch_entries(streaming_query)
        print(f"[DEBUG] Streaming setup done in {time.time()-t0:.1f}s", flush=True)

        if log_progress:
            batch_iter = tqdm(
                batch_iter,
                total=num_batches,
                desc="Pinecone update",
                unit="batch",
                dynamic_ncols=True,
            )

        total_processed = 0
        batch_num = 0
        for batch in batch_iter:
            batch_num += 1
            print(f"[DEBUG] Got batch {batch_num}, {len(batch)} items, processing...", flush=True)
            t0 = time.time()
            self.save_batch(session, batch)
            print(f"[DEBUG] Batch {batch_num} saved in {time.time()-t0:.1f}s", flush=True)
            total_processed += len(batch)
            if log_progress and hasattr(batch_iter, 'set_postfix'):
                batch_iter.set_postfix(articles=total_processed)

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
        - Embeddings are cached (vector_cache.py) so no re-embedding needed
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
        # Next run will re-process (embeddings cached, so just DB write + Pinecone upsert)
        logger.error(
            f"MySQL connection failed after {max_retries} attempts. "
            f"Batch will be reprocessed on next run (embeddings are cached). Error: {last_error}"
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
        import time
        items = iter(article_stream)
        batch_num = 0
        while True:
            print(f"[DEBUG] batch_entries: fetching next {self.batch_size} articles from stream...", flush=True)
            t0 = time.time()
            batch = list(islice(items, self.batch_size))
            if not batch:
                print(f"[DEBUG] batch_entries: stream exhausted", flush=True)
                break
            batch_num += 1
            print(f"[DEBUG] batch_entries: got {len(batch)} articles in {time.time()-t0:.1f}s, embedding...", flush=True)
            t0 = time.time()
            result = self._embed_batch(batch)
            print(f"[DEBUG] batch_entries: embedded in {time.time()-t0:.1f}s", flush=True)
            yield result

    def _embed_batch(
        self, articles: List[Article]
    ) -> List[Tuple[Article, PineconeEntry | None]]:
        """Embed a batch of articles together using contextualized embeddings."""
        logger.info("Embedding batch of %s articles", len(articles))
        import time
        print(f"[DEBUG] _embed_batch: chunking {len(articles)} articles...", flush=True)

        # Build {article: chunks} for articles with content (now returns Chunk objects)
        t0 = time.time()
        chunks_by_article = {a: get_raw_chunks(a) for a in articles}
        valid_articles = [a for a in articles if chunks_by_article[a]]
        total_chunks = sum(len(chunks_by_article[a]) for a in valid_articles)
        print(f"[DEBUG] _embed_batch: chunked in {time.time()-t0:.1f}s, {len(valid_articles)} valid articles, {total_chunks} total chunks", flush=True)
        for a in articles:
            if not chunks_by_article[a]:
                logger.warning(f"No chunks for {a.title}")

        if not valid_articles:
            return [(a, None) for a in articles]

        try:
            # Extract text strings for embedding API (Chunk.value)
            texts_by_article = [[c.value for c in chunks_by_article[a]] for a in valid_articles]
            print(f"[DEBUG] _embed_batch: calling Voyage API...", flush=True)
            t0 = time.time()
            all_embeddings = embed_documents_contextualized(texts_by_article)
            print(f"[DEBUG] _embed_batch: Voyage API returned in {time.time()-t0:.1f}s", flush=True)
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
