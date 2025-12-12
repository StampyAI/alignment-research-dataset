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
Embedding = namedtuple("Embedding", ["vector", "text"])
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
    return [Embedding(vector=v, text=t) for t, v in zip(texts, vectors)]


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

    # Batch by voyage-context-3 limits
    # Use cleaned_docs for embedding, then restore alignment
    cleaned_results = []
    batch = []
    batch_tokens = 0
    batch_chunks = 0

    # Be conservative: ~3 chars/token to avoid underestimating technical content
    # Also use 80% of MAX_EMBEDDING_TOKENS as safety margin (120k -> 96k)
    safe_token_limit = int(MAX_EMBEDDING_TOKENS * 0.8)

    for doc in cleaned_docs:
        # Conservative token estimate: ~3 chars/token for technical content
        doc_tokens = sum(len(chunk) for chunk in doc) // 3 + len(doc)
        doc_chunks = len(doc)

        # If adding this doc would exceed limits, process current batch first
        if batch and (
            len(batch) >= 1000  # Max 1000 documents per request
            or batch_tokens + doc_tokens > safe_token_limit
            or batch_chunks + doc_chunks > 16000
        ):
            batch_result = _contextualized_embed_batch(batch, model, input_type)
            cleaned_results.extend(batch_result)
            batch = []
            batch_tokens = 0
            batch_chunks = 0

        batch.append(doc)
        batch_tokens += doc_tokens
        batch_chunks += doc_chunks

    # Process final batch
    if batch:
        batch_result = _contextualized_embed_batch(batch, model, input_type)
        cleaned_results.extend(batch_result)

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
