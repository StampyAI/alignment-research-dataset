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
    """Batch texts into chunks of max_tokens."""
    batch = []
    tokens = 0
    for text in texts:
        tokens += len(text)
        if tokens > max_tokens:
            yield batch
            batch = []
            tokens = 0
        batch.append(text)
    if batch:
        yield batch


def embed_texts(texts: list[str]) -> list[Embedding]:
    """
    Obtain embeddings without moderation checks.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - source (Optional[str], optional): Source identifier to potentially adjust embedding bias. Defaults to None.
    - **kwargs: Additional keyword arguments passed to the embedding function.

    Returns:
    - List[List[float]]: List of embeddings for the provided texts.
    """
    if not texts or not voyageai_client:
        return []

    texts = [text.replace("\n", " ") for text in texts]

    def e_b(batch, model):
        try:
            return voyageai_client.embed(batch, model=model).embeddings
        except Exception as e:
            logger.error(f"Error embedding batch: {e}")
            logger.error(f"Batch: {batch}")
            raise

    vectors = [
        vector
        for batch in batch_texts(texts, MAX_EMBEDDING_TOKENS)
        if batch
        for vector in e_b(batch, VOYAGEAI_EMBEDDINGS_MODEL)
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
    # replace newlines, which can negatively affect performance
    texts = [text.replace("\n", " ").strip() for text in texts]
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
