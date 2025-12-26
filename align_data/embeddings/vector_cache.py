"""
Disk-backed cache for embedding vectors.

PURPOSE:
Prevents expensive re-computation when embedding jobs fail and restart.
Voyage API calls cost $0.18/MTok - recomputing 50K articles would cost ~$34.

HOW IT WORKS:
- SQLite database at ~/.cache/ard_embeddings/embedding_cache.db
- Cache key: (doc_hash, model, input_type) where doc_hash = SHA256(JSON(chunks))
- Stores compressed numpy arrays (zlib level 6)

WHY input_type IN CACHE KEY:
voyage-context-3 produces DIFFERENT vectors for input_type="document" (indexing)
vs input_type="query" (search). If cached under same key, search would fail.

SCHEMA VERSIONING:
SCHEMA_VERSION constant (currently 2) auto-clears cache on incompatible changes.
Increment when: changing hash algorithm, storage format, or key structure.

NO DATA LOSS RISK:
- Cache is read-only fallback - if lookup fails, we just re-embed
- Cache corruption → cache miss → re-embed (self-healing)
- Wrong cache → validated by chunk count match → cache miss if mismatch
"""

import hashlib
import io
import json
import logging
import os
import sqlite3
import zlib
from datetime import datetime
from pathlib import Path
from typing import Callable

import numpy as np

logger = logging.getLogger(__name__)

# Cache location: configurable via env var, defaults to ~/.cache/ard_embeddings/
CACHE_DIR = Path(os.environ.get(
    "ARD_EMBEDDING_CACHE_DIR",
    Path.home() / ".cache" / "ard_embeddings"
))
CACHE_DB_PATH = CACHE_DIR / "embedding_cache.db"

# Schema version - increment when schema changes to auto-clear incompatible caches
SCHEMA_VERSION = 2  # v2: added input_type to composite key


def _ensure_cache_dir():
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _get_connection() -> sqlite3.Connection:
    """Get SQLite connection, creating schema if needed."""
    _ensure_cache_dir()
    conn = sqlite3.connect(str(CACHE_DB_PATH), timeout=30.0)

    # Check schema version - clear cache if outdated
    current_version = conn.execute("PRAGMA user_version").fetchone()[0]
    if current_version != SCHEMA_VERSION:
        logger.info(f"Cache schema version mismatch ({current_version} != {SCHEMA_VERSION}), clearing cache")
        conn.execute("DROP TABLE IF EXISTS embeddings")
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.commit()

    # Create table with composite primary key: (doc_hash, model, input_type)
    # input_type distinguishes "document" vs "query" embeddings (different vectors!)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS embeddings (
            doc_hash TEXT NOT NULL,
            model TEXT NOT NULL,
            input_type TEXT NOT NULL,
            vectors BLOB NOT NULL,
            chunk_count INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (doc_hash, model, input_type)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_model ON embeddings(model)")
    conn.commit()
    return conn


def hash_document(chunks: list[str]) -> str:
    """
    Compute deterministic hash of a document (list of chunks).

    DETERMINISM:
    - json.dumps with fixed separators is deterministic for lists of strings
    - ensure_ascii=False preserves Unicode exactly
    - SHA256 is collision-resistant (2^128 security level)

    WHY HASH CHUNKS, NOT ARTICLE ID:
    - Article ID doesn't change when content changes
    - If text_splitter settings change, chunks change, hash changes → cache miss
    - This ensures cache invalidation when chunking logic changes
    """
    # JSON serialization is deterministic for lists of strings
    content = json.dumps(chunks, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def _serialize_vectors(vectors: list[list[float]]) -> bytes:
    """Serialize vectors to compressed bytes."""
    arr = np.array(vectors, dtype=np.float32)
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    return zlib.compress(buf.getvalue(), level=6)


def _deserialize_vectors(data: bytes) -> list[list[float]]:
    """Deserialize vectors from compressed bytes."""
    buf = io.BytesIO(zlib.decompress(data))
    arr = np.load(buf, allow_pickle=False)
    return arr.tolist()


def get_cached_vectors(doc_hash: str, model: str, input_type: str) -> list[list[float]] | None:
    """
    Look up cached vectors for a document hash.

    Returns None if not cached or cached for different model/input_type.

    Args:
        doc_hash: SHA256 hash of document chunks
        model: Embedding model name (e.g., "voyage-context-3")
        input_type: "document" for indexing or "query" for search (produces different embeddings!)
    """
    conn = None
    try:
        conn = _get_connection()
        cursor = conn.execute(
            "SELECT vectors FROM embeddings WHERE doc_hash = ? AND model = ? AND input_type = ?",
            (doc_hash, model, input_type)
        )
        row = cursor.fetchone()

        if row:
            return _deserialize_vectors(row[0])
        return None
    except (sqlite3.Error, zlib.error, ValueError) as e:
        logger.warning(f"Cache lookup failed for {doc_hash[:16]}...: {e}")
        return None
    finally:
        if conn:
            conn.close()


def store_vectors(doc_hash: str, model: str, input_type: str, vectors: list[list[float]], chunk_count: int):
    """
    Store vectors in cache.

    Uses INSERT OR REPLACE to handle duplicates gracefully.

    Args:
        doc_hash: SHA256 hash of document chunks
        model: Embedding model name
        input_type: "document" or "query"
        vectors: List of embedding vectors
        chunk_count: Number of chunks (for validation on retrieval)
    """
    conn = None
    try:
        conn = _get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO embeddings (doc_hash, model, input_type, vectors, chunk_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (doc_hash, model, input_type, _serialize_vectors(vectors), chunk_count, datetime.utcnow().isoformat())
        )
        conn.commit()
    except (sqlite3.Error, zlib.error) as e:
        logger.warning(f"Cache store failed for {doc_hash[:16]}...: {e}")
    finally:
        if conn:
            conn.close()


def cached_embed_documents(
    documents: list[list[str]],
    model: str,
    input_type: str,
    embed_fn: Callable[[list[list[str]]], list[list[list[float]]]]
) -> list[list[list[float]]]:
    """
    Embed documents with caching.

    For each document, checks cache first. Only calls embed_fn for uncached documents.
    Results are stored in cache for future runs.

    Args:
        documents: List of documents, each document is a list of chunk strings
        model: Embedding model name (cache key includes model)
        input_type: "document" for indexing or "query" for search - CRITICAL: these
                    produce different embeddings and must be cached separately!
        embed_fn: Function that takes list of documents and returns list of embeddings

    Returns:
        List of embeddings matching input documents structure
    """
    if not documents:
        return []

    # Check cache for each document
    results = [None] * len(documents)
    uncached_indices = []
    uncached_docs = []
    cache_hits = 0

    for i, doc in enumerate(documents):
        if not doc:  # Empty document
            results[i] = []
            continue

        doc_hash = hash_document(doc)
        cached = get_cached_vectors(doc_hash, model, input_type)

        if cached is not None and len(cached) == len(doc):
            results[i] = cached
            cache_hits += 1
        else:
            uncached_indices.append(i)
            uncached_docs.append(doc)

    if cache_hits > 0:
        logger.info(f"Embedding cache: {cache_hits} hits, {len(uncached_docs)} misses")

    # Embed uncached documents
    if uncached_docs:
        new_embeddings = embed_fn(uncached_docs)

        # Store in cache and results
        for idx, (orig_idx, doc) in enumerate(zip(uncached_indices, uncached_docs)):
            vectors = new_embeddings[idx]
            results[orig_idx] = vectors

            # Store in cache
            doc_hash = hash_document(doc)
            store_vectors(doc_hash, model, input_type, vectors, len(doc))

    return results


def get_cache_stats() -> dict:
    """Get cache statistics."""
    conn = None
    try:
        conn = _get_connection()
        cursor = conn.execute("""
            SELECT model, COUNT(*) as count, SUM(chunk_count) as total_chunks
            FROM embeddings
            GROUP BY model
        """)
        rows = cursor.fetchall()

        total_size = conn.execute(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()"
        ).fetchone()[0]

        return {
            "by_model": {row[0]: {"documents": row[1], "chunks": row[2]} for row in rows},
            "total_documents": sum(row[1] for row in rows),
            "total_chunks": sum(row[2] for row in rows),
            "cache_size_bytes": total_size,
            "cache_path": str(CACHE_DB_PATH),
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()


def clear_cache(model: str | None = None):
    """Clear cache, optionally for specific model only."""
    conn = None
    try:
        conn = _get_connection()
        if model:
            conn.execute("DELETE FROM embeddings WHERE model = ?", (model,))
        else:
            conn.execute("DELETE FROM embeddings")
        conn.commit()
        conn.execute("VACUUM")
        conn.commit()
    finally:
        if conn:
            conn.close()
