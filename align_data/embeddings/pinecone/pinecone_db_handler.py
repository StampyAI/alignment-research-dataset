# dataset/pinecone_db_handler.py
import time
import logging
import re
from typing import List, Tuple

from pinecone import Pinecone, ServerlessSpec
from pinecone.exceptions import PineconeApiException
from urllib3.exceptions import ProtocolError

from align_data.embeddings.embedding_utils import get_embedding
from align_data.embeddings.pinecone.pinecone_models import PineconeEntry
from align_data.settings import (
    PINECONE_INDEX_NAME,
    EMBEDDINGS_DIMS,
    PINECONE_METRIC,
    PINECONE_API_KEY,
    PINECONE_ENVIRONMENT,
    PINECONE_NAMESPACE,
)

logger = logging.getLogger(__name__)


def chunk_items(items, chunk_size):
    """Split items into chunks of specified size."""
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def with_retry(n=3, exceptions=(Exception,)):
    """Basic retry decorator with limited attempts."""
    def retrier_wrapper(f):
        def wrapper(*args, **kwargs):
            for i in range(n):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    logger.error(f"Got exception while retrying: {e}")
                time.sleep(2**i)
            raise TimeoutError(f"Gave up after {n} tries")

        return wrapper

    return retrier_wrapper


def with_infinite_retry_on_429(f):
    """
    Retry decorator for Pinecone operations that retries indefinitely on 429 errors.

    FIXES ERROR: "429 Too Many Requests" and "You've reached the max storage allowed"
    Both return HTTP 429 from Pinecone.

    WHY INFINITE RETRY:
    - Rate limits: Transient, resolve with backoff. Infinite retry = auto-recovery.
    - Storage limits: Needs manual intervention. Infinite retry = process pauses
      waiting for human to upgrade plan/delete data, rather than crashing.

    WHY THIS LOCATION (on _upsert only):
    - Upserts are the main bottleneck during indexing
    - Deletes use plain @with_retry - if delete hits 429 and fails, batch stays
      in pending_removal status and will be retried on next run (no data loss)

    DECORATOR STACKING (line 167-168):
        @with_infinite_retry_on_429  <- outer, catches PineconeApiException
        @with_retry(exceptions=(ProtocolError,))  <- inner, catches network errors
    Inner runs first. If ProtocolError 3x → TimeoutError → outer re-raises (don't
    infinitely retry network failures, they're likely persistent).
    """
    def wrapper(*args, **kwargs):
        attempt = 0
        max_delay = 300  # 5 minutes max between retries

        while True:
            try:
                return f(*args, **kwargs)
            except PineconeApiException as e:
                # Use getattr for safety - some Pinecone exception versions may differ
                status = getattr(e, 'status', None)
                if status == 429:
                    attempt += 1
                    # Exponential backoff: 2, 4, 8, 16, 32, 64, 128, 256, 300, 300...
                    delay = min(2 ** attempt, max_delay)

                    # Distinguish storage limit vs rate limit for logging only.
                    # Both are retried indefinitely - the distinction is just for
                    # clearer log messages to help operators understand what's happening.
                    # FRAGILE: String matching could break if Pinecone changes error format.
                    error_body = str(e.body) if (hasattr(e, 'body') and e.body is not None) else str(e)
                    is_storage_limit = 'storage' in error_body.lower() and 'limit' in error_body.lower()

                    if is_storage_limit:
                        logger.warning(
                            f"Pinecone STORAGE LIMIT reached (attempt {attempt}). "
                            f"Waiting {delay}s for manual intervention. "
                            f"Options: upgrade plan, delete old index, or free up space. "
                            f"Error: {error_body[:200]}"
                        )
                    else:
                        logger.warning(
                            f"Pinecone rate limit hit (attempt {attempt}). "
                            f"Backing off for {delay}s..."
                        )

                    time.sleep(delay)
                else:
                    # Non-429 Pinecone errors (400 bad request, 500 internal, etc.)
                    # Don't retry - likely permanent errors that won't resolve with time
                    raise
            except TimeoutError:
                # TimeoutError comes from inner @with_retry exhausting its attempts.
                # If the inner decorator gave up after 3 tries, the problem is persistent
                # (network issues, DNS failures, etc.) - don't retry indefinitely.
                raise
            except Exception as e:
                # Other transient errors (network issues, etc.) - retry with backoff
                # but NOT indefinitely (cap at 10 attempts) to avoid infinite loops
                # on unexpected persistent errors.
                attempt += 1
                if attempt > 10:
                    logger.error(f"Pinecone operation failed after {attempt} attempts, giving up: {e}")
                    raise
                delay = min(2 ** attempt, 60)  # Max 60s for non-429 errors
                logger.warning(
                    f"Pinecone operation failed (attempt {attempt}): {type(e).__name__}: {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

    return wrapper


def initialize_pinecone():
    """Initialize Pinecone client with compatibility for both API versions."""
    try:
        return Pinecone(
            api_key=PINECONE_API_KEY,
            environment=PINECONE_ENVIRONMENT,
        )
    except Exception as e:
        logger.error(f"Failed to initialize Pinecone: {e}")
        raise


class PineconeDB:
    def __init__(
        self,
        index_name: str = PINECONE_INDEX_NAME,
        values_dims: int = EMBEDDINGS_DIMS,
        metric: str = PINECONE_METRIC,
        create_index: bool = False,
        log_index_stats: bool = False,
    ):
        self.index_name = index_name
        self.values_dims = values_dims
        self.metric = metric

        # Initialize Pinecone with version compatibility
        self.pinecone = initialize_pinecone()

        if create_index:
            self.create_index()

        # Get index with version compatibility
        try:
            self.index = self.pinecone.Index(self.index_name)
        except Exception as e:
            logger.error(f"Failed to access Pinecone index: {e}")
            raise

        if log_index_stats:
            try:
                index_stats_response = self.index.describe_index_stats()
                logger.info(f"{self.index_name}:\n{index_stats_response}")
            except Exception as e:
                logger.error(f"Failed to get index stats: {e}")

    @with_retry(exceptions=(ProtocolError,))
    def _get_vectors(self, entry: PineconeEntry) -> list[dict]:
        return entry.create_pinecone_vectors()

    @with_infinite_retry_on_429
    @with_retry(exceptions=(ProtocolError,))
    def _upsert(
        self, vectors: list[dict], upsert_size: int = 100, show_progress: bool = True
    ):
        try:
            self.index.upsert(
                vectors=vectors,
                batch_size=upsert_size,
                namespace=PINECONE_NAMESPACE,
                show_progress=show_progress,
            )
        except Exception as e:
            logger.error(f"Failed to upsert vectors: {e}")
            raise

    def upsert_entry(
        self,
        pinecone_entry: PineconeEntry,
        upsert_size: int = 100,
        show_progress: bool = True,
    ):
        vectors = self._get_vectors(pinecone_entry)
        self._upsert(vectors, upsert_size, show_progress)

    def query_vector(
        self,
        query: List[float],
        top_k: int = 10,
        include_values: bool = False,
        include_metadata: bool = True,
        **kwargs,
    ) -> List[dict]:
        assert not isinstance(query, str), (
            "query must be a list of floats. Use query_PineconeDB_text for text queries"
        )

        try:
            query_response = self.index.query(
                vector=query,
                top_k=top_k,
                include_values=include_values,
                include_metadata=include_metadata,
                **kwargs,
                namespace=PINECONE_NAMESPACE,
            )
        except (TypeError, KeyError) as e:
            # Handle API differences
            logger.warning(
                f"Query API compatibility issue: {e}, attempting alternate approach"
            )
            try:
                # Try alternative parameter format for older API
                kwargs.pop(
                    "namespace", None
                )  # In case namespace is handled differently
                query_response = self.index.query(
                    vector=query,
                    top_k=top_k,
                    include_values=include_values,
                    include_metadata=include_metadata,
                    namespace=PINECONE_NAMESPACE,
                    **kwargs,
                )
            except Exception as inner_e:
                logger.error(f"Failed to query with alternative approach: {inner_e}")
                raise

        try:
            # Extract matches with compatibility for different response formats
            matches = query_response.get("matches", [])
            if not matches and hasattr(query_response, "matches"):
                matches = query_response.matches  # Some versions return an object

            result = []
            for match in matches:
                # Handle both dictionary and object formats
                match_id = (
                    match.get("id")
                    if isinstance(match, dict)
                    else getattr(match, "id", None)
                )
                match_score = (
                    match.get("score")
                    if isinstance(match, dict)
                    else getattr(match, "score", None)
                )

                # Handle metadata in both formats
                if isinstance(match, dict):
                    metadata = match.get("metadata", {})
                else:
                    metadata = getattr(match, "metadata", {})

                # Create a dictionary representation for consistent interface
                result.append(
                    {
                        "id": match_id,
                        "score": match_score,
                        "metadata": metadata,  # We'll use the metadata directly instead of wrapping in PineconeMetadata
                    }
                )

            return result
        except Exception as e:
            logger.error(f"Failed to parse query response: {e}")
            raise

    def query_text(
        self,
        query: str,
        top_k: int = 10,
        include_values: bool = False,
        include_metadata: bool = True,
        **kwargs,
    ) -> List[dict]:
        query_vectors, _ = get_embedding(query)
        if not query_vectors:
            print("The query is invalid.")
            return []

        return self.query_vector(
            query=query_vectors[0].vector,
            top_k=top_k,
            include_values=include_values,
            include_metadata=include_metadata,
            **kwargs,
        )

    @with_retry()
    def _find_item(self, id_):
        """Find all vector IDs with the given prefix."""
        return list(self.index.list(prefix=id_, namespace=PINECONE_NAMESPACE))

    @with_retry()
    def _del_items(self, ids):
        """Delete vector IDs from Pinecone.

        Pinecone has a limit of 1000 IDs per delete request.
        """
        max_delete_size = 1000

        for chunk in chunk_items(ids, max_delete_size):
            self.index.delete(ids=chunk, namespace=PINECONE_NAMESPACE)

    @with_retry()
    def delete_entries(self, ids):
        """Delete entries from Pinecone by their hash_ids.

        Each hash_id may expand to multiple vector IDs. This method processes
        article IDs in small chunks to avoid memory issues and handles the
        expanded vector IDs to avoid exceeding Pinecone's 1000 ID limit per request.
        """
        article_chunk_size = 10

        for chunk in chunk_items(ids, article_chunk_size):
            # Find all vector IDs for these articles
            vector_ids = []
            for article_id in chunk:
                article_vectors = self._find_item(article_id)
                if article_vectors:
                    vector_ids.extend(article_vectors)

            if vector_ids:
                self._del_items(vector_ids)

    def create_index(self):
        try:
            self.pinecone.create_index(
                name=self.index_name,
                dimension=self.values_dims,
                metric=self.metric,
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            raise

    def delete_index(self):
        try:
            indexes = []
            indexes = self.pinecone.list_indexes()

            if self.index_name in indexes:
                logger.info(f"Deleting index '{self.index_name}'.")
                self.pinecone.delete_index(self.index_name)
        except Exception as e:
            logger.error(f"Failed to delete index: {e}")
            raise

    def get_embeddings_by_ids(
        self, ids: List[str]
    ) -> List[Tuple[str, List[float] | None]]:
        """
        Fetch embeddings for given entry IDs from Pinecone.

        Args:
        - ids (List[str]): List of entry IDs for which embeddings are to be fetched.

        Returns:
        - List[Tuple[str, List[float] | None]]: List of tuples containing ID and its corresponding embedding.
        """
        try:
            # Try new API format
            fetch_response = self.index.fetch(
                ids=ids,
                namespace=PINECONE_NAMESPACE,
            )

            # Handle different response formats
            if isinstance(fetch_response, dict) and "vectors" in fetch_response:
                # Dictionary response
                vectors = fetch_response["vectors"]
                return [(id, vectors.get(id, {}).get("values", None)) for id in ids]
            elif hasattr(fetch_response, "vectors"):
                # Object response
                vectors = fetch_response.vectors
                if isinstance(vectors, dict):
                    return [(id, vectors.get(id, {}).get("values", None)) for id in ids]
                else:
                    # Handle other vector formats
                    return [
                        (id, getattr(vectors.get(id, {}), "values", None)) for id in ids
                    ]
            else:
                logger.error(f"Unexpected response format: {type(fetch_response)}")
                raise ValueError(f"Unexpected response format: {type(fetch_response)}")

        except Exception as e:
            logger.error(f"Error fetching embeddings: {e}")
            # Attempt alternative approach with more error details
            logger.warning("Attempting alternative fetch approach")
            try:
                # Try alternative format for the fetch call
                fetch_response = self.index.fetch(ids=ids, namespace=PINECONE_NAMESPACE)
                if hasattr(fetch_response, "vectors"):
                    vectors = fetch_response.vectors
                else:
                    vectors = fetch_response.get("vectors", {})
                return [(id, vectors.get(id, {}).get("values", None)) for id in ids]
            except Exception as inner_e:
                logger.error(f"Alternative fetch approach failed: {inner_e}")
                raise


def strip_block(text: str) -> str:
    return "\n".join(text.split("\n")[1:])
