import os
os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")

import logging
from collections import namedtuple
from typing import Any, Generator
from functools import wraps

from openai import OpenAI

from openai import (
    OpenAIError,
    RateLimitError,
    APIError,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
    retry_if_exception,
)
import voyageai

from align_data.settings import (
    OPENAI_API_KEY,
    OPENAI_ORGANIZATION,
    VOYAGEAI_API_KEY,
    VOYAGEAI_EMBEDDINGS_MODEL,
    USE_MODERATION,
    MAX_EMBEDDING_TOKENS,
)
from align_data.embeddings.vector_cache import cached_embed_documents

if OPENAI_API_KEY:
    openai_client = OpenAI(api_key=OPENAI_API_KEY, organization=OPENAI_ORGANIZATION)
else:
    openai_client = None

if VOYAGEAI_API_KEY:
    voyageai_client = voyageai.Client(api_key=VOYAGEAI_API_KEY)
else:
    voyageai_client = None

# --------------------
# CONSTANTS & CONFIGURATION
# --------------------

logger = logging.getLogger(__name__)

ModerationInfoType = dict[str, Any]
Embedding = namedtuple("Embedding", [
    "vector", "text",
    "section_heading"  # from text_splitter.Chunk
])
Vector = list[float]


# --------------------
# DECORATORS
# --------------------


def handle_openai_errors(func):
    """Decorator to handle OpenAI-specific exceptions with retries."""

    @wraps(func)
    @retry(
        wait=wait_random_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(6),
        retry=retry_if_exception_type(RateLimitError)
        | retry_if_exception_type(APIError)
        | retry_if_exception(lambda e: "502" in str(e)),
    )
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RateLimitError as e:
            logger.warning(f"OpenAI Rate limit error. Trying again. Error: {e}")
            raise
        except APIError as e:
            if "502" in str(e):
                logger.warning(
                    f"OpenAI 502 Bad Gateway error. Trying again. Error: {e}"
                )
            else:
                logger.error(f"OpenAI API Error encountered: {e}")
            raise
        except OpenAIError as e:
            logger.error(f"OpenAI Error encountered: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error encountered: {e}")
            raise

    return wrapper


# --------------------
# MAIN FUNCTIONS
# --------------------


@handle_openai_errors
def _single_batch_moderation_check(batch: list[str]) -> list[ModerationInfoType]:
    """Process a batch for moderation checks."""
    if not openai_client:
        return []
    return openai_client.moderations.create(input=batch).results


def moderation_check(texts: list[str]) -> list[ModerationInfoType]:
    """Batch moderation checks on list of texts.

    :param List[str] texts: the texts to be checked
    :param int max_batch_size: the max size in tokens for a single batch
    :param Callable[[str], int] tokens_counter: the function used to count tokens
    """
    return [
        result for text in texts for result in _single_batch_moderation_check([text])
    ]


def batch_texts(texts: list[str], max_tokens: int) -> Generator[list[str], None, None]:
    """Batch texts into chunks of max_tokens (using char length as proxy)."""
    batch = []
    tokens = 0
    for text in texts:
        text_len = len(text)
        if text_len > max_tokens:
            logger.warning(f"Single text exceeds max_tokens ({text_len} > {max_tokens}), sending anyway")
        if tokens + text_len > max_tokens and batch:
            yield batch
            batch = []
            tokens = 0
        batch.append(text)
        tokens += text_len
    if batch:
        yield batch


def embed_texts(texts: list[str]) -> list[Embedding]:
    """
    Obtain embeddings without moderation checks.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - model: Voyage model to use (default from settings)

    Returns:
    - List[Embedding]: List of embeddings for the provided texts.
    """
    if not texts or not voyageai_client:
        return []

    model = VOYAGEAI_EMBEDDINGS_MODEL

    use_contextualized = model == "voyage-context-3"

    def e_b(batch):
        if use_contextualized:
            # Wrap each text as single-chunk document for contextualized API
            results = _contextualized_embed_batch([[t] for t in batch], model, "document")
            return [doc[0] for doc in results]
        else:
            return voyageai_client.embed(batch, model=model).embeddings

    vectors = [
        vector
        for batch in batch_texts(texts, MAX_EMBEDDING_TOKENS)
        if batch
        for vector in e_b(batch)
    ]
    # Legacy path: no chunk metadata available, use defaults
    return [Embedding(vector=v, text=t, section_heading=None)
            for t, v in zip(texts, vectors)]


def get_embeddings(
    texts: list[str],
) -> tuple[list[Embedding], list[ModerationInfoType]]:
    """
    Obtain embeddings for the provided texts, replacing the embeddings of flagged texts with `None`.

    Parameters:
    - texts (List[str]): List of texts to be embedded.

    Returns:
    - Tuple[List[Optional[List[float]]], ModerationInfoListType]: Tuple containing the list of embeddings (with None for flagged texts) and the moderation results.
    """
    texts = [text.strip() for text in texts]
    texts = [text for text in texts if text]
    if not texts:
        return [], []

    # Check all texts for moderation flags
    if not USE_MODERATION:
        return embed_texts(texts), []

    moderation_results = moderation_check(texts)
    # If moderation unavailable (no OpenAI key), treat all as OK
    if not moderation_results:
        return embed_texts(texts), []

    flagged = [
        dict(result) | {"text": t}
        for t, result in zip(texts, moderation_results)
        if result.flagged
    ]

    ok = [t for t, r in zip(texts, moderation_results) if not r.flagged]
    vectors = ok and embed_texts(ok)
    return vectors, flagged


def get_embedding(text: str) -> tuple[list[Embedding], list[ModerationInfoType]]:
    """Obtain an embedding for a single text."""
    embedding, moderation_result = get_embeddings([text])
    return embedding, moderation_result


# --------------------
# CONTEXTUALIZED EMBEDDINGS (voyage-context-3)
# --------------------


def embed_documents_contextualized(
    documents: list[list[str]],
    input_type: str = "document",
) -> list[list[Vector]]:
    """
    Embed documents using voyage-context-3 contextualized embeddings.

    Unlike embed_texts(), this function takes documents as lists of chunks,
    where each chunk is aware of its siblings in the same document.

    Parameters:
    - documents: List of documents, each document is a list of chunk strings
    - input_type: "document" for indexing, "query" for search

    Returns:
    - List of lists of vectors, matching the input structure exactly.
      Empty documents return empty lists. Maintains 1:1 correspondence with input.

    Rate limits per request:
    - Maximum 1,000 documents (inner lists)
    - Maximum 120,000 total tokens
    - Maximum 16,000 total chunks
    """
    if not voyageai_client:
        return [[] for _ in documents]
    if not documents:
        return []

    model = VOYAGEAI_EMBEDDINGS_MODEL

    # Validate chunks - no empty/whitespace-only chunks allowed
    # Caller must filter these before calling; we crash to surface bugs
    for i, doc in enumerate(documents):
        for j, chunk in enumerate(doc):
            if not (chunk and chunk.strip()):
                raise ValueError(
                    f"Empty chunk at doc[{i}][{j}] - caller must filter empty chunks"
                )

    # Track which documents are non-empty so we can restore alignment later
    cleaned_docs = []
    doc_indices = []  # Maps cleaned index -> original index
    for i, doc in enumerate(documents):
        if doc:
            cleaned_docs.append(doc)
            doc_indices.append(i)

    if not cleaned_docs:
        return [[] for _ in documents]

    # Use cache-aware embedding - checks cache per-document, only embeds uncached
    # IMPORTANT: input_type is part of cache key because "document" and "query"
    # produce different embeddings from voyage-context-3
    def embed_uncached(docs_to_embed: list[list[str]]) -> list[list[Vector]]:
        """Embed documents without caching (called for cache misses)."""
        return _embed_documents_batched(docs_to_embed, model, input_type)

    cleaned_results = cached_embed_documents(cleaned_docs, model, input_type, embed_uncached)

    # Verify we got results for all documents - fail loudly if not
    if len(cleaned_results) != len(cleaned_docs):
        raise RuntimeError(
            f"Embedding count mismatch: got {len(cleaned_results)} results "
            f"for {len(cleaned_docs)} documents. Partial batch failure?"
        )

    # Restore alignment: map cleaned results back to original document positions
    results = [[] for _ in documents]
    for cleaned_idx, orig_idx in enumerate(doc_indices):
        results[orig_idx] = cleaned_results[cleaned_idx]

    return results


def _embed_documents_batched(
    documents: list[list[str]],
    model: str,
    input_type: str,
) -> list[list[Vector]]:
    """
    Batch and embed documents respecting API limits.

    This is the uncached embedding path - called for cache misses.

    FIXES ERROR: "The example at index N in your batch has too many tokens and
    does not fit into the model's context window of 32000"

    HOW IT FIXES:
    1. Before batching, estimate each document's token count
    2. If document > 30K tokens, split into sub-documents
    3. Each sub-document is embedded separately (loses some cross-chunk context)
    4. Results are recombined by original document index

    WHY 30K NOT 32K:
    - Leave 2K headroom (~6000 chars) for API overhead and estimation error
    - Token estimation is crude (~3 chars/token), could underestimate

    TRADE-OFF:
    - Split documents lose cross-chunk context (voyage-context-3's main benefit)
    - But partial context is better than complete failure
    """
    # voyage-context-3 has 32K context window per document
    # Leave headroom for API overhead and estimation error
    MAX_DOC_TOKENS = 30000

    # First, handle oversized documents by splitting them
    processed_docs = []  # List of (original_idx, doc_chunks) tuples
    for orig_idx, doc in enumerate(documents):
        doc_tokens = _estimate_tokens(doc)

        if doc_tokens <= MAX_DOC_TOKENS:
            # Document fits in context window
            processed_docs.append((orig_idx, doc))
        else:
            # Document too large - split into sub-documents
            logger.info(
                f"Document {orig_idx} has ~{doc_tokens} tokens (>{MAX_DOC_TOKENS}), "
                f"splitting into sub-batches"
            )
            sub_docs = _split_oversized_document(doc, MAX_DOC_TOKENS)
            for sub_doc in sub_docs:
                processed_docs.append((orig_idx, sub_doc))

    # Now batch the processed documents for API calls
    results_by_orig_idx: dict[int, list[Vector]] = {i: [] for i in range(len(documents))}
    batch = []
    batch_indices = []  # Track which original doc each batch item belongs to
    batch_tokens = 0
    batch_chunks = 0

    # Be conservative: ~3 chars/token to avoid underestimating technical content
    # Also use 80% of MAX_EMBEDDING_TOKENS as safety margin (120k -> 96k)
    safe_token_limit = int(MAX_EMBEDDING_TOKENS * 0.8)

    for orig_idx, doc in processed_docs:
        doc_tokens = _estimate_tokens(doc)
        doc_chunks = len(doc)

        # If adding this doc would exceed limits, process current batch first
        if batch and (
            len(batch) >= 1000  # Max 1000 documents per request
            or batch_tokens + doc_tokens > safe_token_limit
            or batch_chunks + doc_chunks > 16000
        ):
            batch_result = _contextualized_embed_batch(batch, model, input_type)
            for idx, vectors in zip(batch_indices, batch_result):
                results_by_orig_idx[idx].extend(vectors)
            batch = []
            batch_indices = []
            batch_tokens = 0
            batch_chunks = 0

        batch.append(doc)
        batch_indices.append(orig_idx)
        batch_tokens += doc_tokens
        batch_chunks += doc_chunks

    # Process final batch
    if batch:
        batch_result = _contextualized_embed_batch(batch, model, input_type)
        for idx, vectors in zip(batch_indices, batch_result):
            results_by_orig_idx[idx].extend(vectors)

    # Validate: embedding count must match chunk count for each document
    # This catches bugs in splitting/recombining and partial API failures
    for i, doc in enumerate(documents):
        expected_chunks = len(doc)
        actual_embeddings = len(results_by_orig_idx[i])
        if expected_chunks != actual_embeddings:
            raise RuntimeError(
                f"Document {i} chunk/embedding count mismatch: "
                f"{expected_chunks} chunks but {actual_embeddings} embeddings. "
                f"This indicates a bug in document splitting or partial API failure."
            )

    # Reconstruct results in original document order
    return [results_by_orig_idx[i] for i in range(len(documents))]


def _estimate_tokens(chunks: list[str]) -> int:
    """
    Estimate token count for a list of chunks.

    APPROXIMATION: ~3 chars/token for English technical content.
    - Underestimates for long technical words (e.g., "contextualized" = 1 token, 14 chars)
    - Overestimates for common short words and punctuation
    - For this codebase (English AI safety content), works reasonably well

    The +len(chunks) accounts for BPE token boundaries at chunk joins.

    RISK: If estimation underestimates by >2K tokens, document won't be split
    when it should be, and API will reject it. The 30K limit (vs 32K actual)
    provides some buffer.
    """
    return sum(len(chunk) for chunk in chunks) // 3 + len(chunks)


def _split_oversized_document(chunks: list[str], max_tokens: int) -> list[list[str]]:
    """
    Split an oversized document into sub-documents that each fit in the context window.

    CONTEXT LOSS:
    Each sub-document is embedded separately. Chunks in sub-doc-2 won't have context
    from sub-doc-1's chunks. This loses voyage-context-3's cross-chunk awareness.
    But partial context (within sub-doc) is better than complete failure.

    CHUNK TRUNCATION (rare edge case):
    If a SINGLE chunk exceeds max_tokens (~90K chars), it's truncated.
    This would require a chunk ~90K chars with our 200-token chunk size setting,
    which shouldn't happen unless text_splitter has a bug.
    Truncation loses data but prevents API failure.

    NOTE ON MUTATION:
    The line `chunk = chunk[:max_chars]` rebinds the local variable, it does NOT
    modify the original list. The truncated string is what gets appended to current_sub.
    """
    sub_docs = []
    current_sub = []
    current_tokens = 0

    for chunk in chunks:
        chunk_tokens = len(chunk) // 3 + 1

        # Handle single chunk exceeding max_tokens (rare edge case)
        if chunk_tokens > max_tokens:
            # Truncate to fit - this loses data but prevents API failure
            # Calculate max chars: max_tokens * 3 (inverse of our estimation)
            max_chars = (max_tokens - 1) * 3
            logger.warning(
                f"Single chunk exceeds context window (~{chunk_tokens} tokens > {max_tokens}), "
                f"truncating from {len(chunk)} to {max_chars} chars"
            )
            chunk = chunk[:max_chars]
            chunk_tokens = max_tokens

        if current_tokens + chunk_tokens > max_tokens and current_sub:
            # Current sub-doc is full, start a new one
            sub_docs.append(current_sub)
            current_sub = []
            current_tokens = 0

        current_sub.append(chunk)
        current_tokens += chunk_tokens

    if current_sub:
        sub_docs.append(current_sub)

    logger.info(f"Split oversized document into {len(sub_docs)} sub-batches")
    return sub_docs


@retry(
    wait=wait_random_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(6),
)
def _contextualized_embed_batch(
    documents: list[list[str]],
    model: str,
    input_type: str,
) -> list[list[Vector]]:
    """Embed a single batch of documents with retry on rate limits."""
    try:
        result = voyageai_client.contextualized_embed(
            inputs=documents,
            model=model,
            input_type=input_type,
        )
        # result.results is a list of objects, each with .embeddings
        return [doc_result.embeddings for doc_result in result.results]
    except Exception as e:
        logger.error(f"Error in contextualized embedding: {e}")
        raise
