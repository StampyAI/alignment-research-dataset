import logging
from typing import List, Tuple, Dict, Any, Optional
from functools import wraps

import openai
from langchain.embeddings import HuggingFaceEmbeddings
from openai.error import (
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

from align_data.embeddings.pinecone.pinecone_models import MissingEmbeddingModelError
from align_data.settings import (
    USE_OPENAI_EMBEDDINGS,
    OPENAI_EMBEDDINGS_MODEL,
    EMBEDDING_LENGTH_BIAS,
    SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
    DEVICE,
)


# --------------------
# CONSTANTS & CONFIGURATION
# --------------------

logger = logging.getLogger(__name__)

hf_embedding_model = None
if not USE_OPENAI_EMBEDDINGS:
    hf_embedding_model = HuggingFaceEmbeddings(
        model_name=SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
        model_kwargs={"device": DEVICE},
        encode_kwargs={"show_progress_bar": False},
    )

ModerationInfoType = Dict[str, Any]


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
                logger.warning(f"OpenAI 502 Bad Gateway error. Trying again. Error: {e}")
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
def moderation_check(texts: List[str], max_texts_num: int = 32) -> List[ModerationInfoType]:
    """
    Check moderation on a list of texts.

    Parameters:
    - texts (List[str]): List of texts to be checked for moderation.
    - max_texts_num (int): Number of texts to check at once. Defaults to 32.

    Returns:
    - List[ModerationInfoType]: List of moderation results for the provided texts.
    """
    total_texts = len(texts)
    results = []

    for i in range(0, total_texts, max_texts_num):
        batch_texts = texts[i : i + max_texts_num]
        batch_results = openai.Moderation.create(input=batch_texts)["results"]
        results.extend(batch_results)

    return results


@handle_openai_errors
def _compute_openai_embeddings(non_flagged_texts: List[str], **kwargs) -> List[List[float]]:
    data = openai.Embedding.create(
        input=non_flagged_texts, engine=OPENAI_EMBEDDINGS_MODEL, **kwargs
    ).data
    return [d["embedding"] for d in data]


def get_embeddings_without_moderation(
    texts: List[str],
    source: Optional[str] = None,
    **kwargs,
) -> List[List[float]]:
    """
    Obtain embeddings without moderation checks.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - source (Optional[str], optional): Source identifier to potentially adjust embedding bias. Defaults to None.
    - **kwargs: Additional keyword arguments passed to the embedding function.

    Returns:
    - List[List[float]]: List of embeddings for the provided texts.
    """
    if not texts:
        return []

    texts = [text.replace("\n", " ") for text in texts]
    if USE_OPENAI_EMBEDDINGS:
        embeddings = _compute_openai_embeddings(texts, **kwargs)
    elif hf_embedding_model:
        embeddings = hf_embedding_model.embed_documents(texts)
    else:
        raise MissingEmbeddingModelError("No embedding model available.")

    # Bias adjustment
    if source and (bias := EMBEDDING_LENGTH_BIAS.get(source, 1.0)):
        embeddings = [[bias * e for e in embedding] for embedding in embeddings]

    return embeddings


def get_embeddings_or_none_if_flagged(
    texts: List[str],
    source: Optional[str] = None,
    **kwargs,
) -> Tuple[List[List[float]] | None, List[ModerationInfoType]]:
    """
    Obtain embeddings for the provided texts. If any text is flagged during moderation,
    the function returns None for the embeddings while still providing the moderation results.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - source (Optional[str], optional): Source identifier to potentially adjust embedding bias. Defaults to None.
    - **kwargs: Additional keyword arguments passed to the embedding function.

    Returns:
    - Tuple[Optional[List[List[float]]], ModerationInfoListType]: Tuple containing the list of embeddings (or None if any text is flagged) and the moderation results.
    """
    moderation_results = moderation_check(texts)
    if any(result["flagged"] for result in moderation_results):
        return None, moderation_results

    embeddings = get_embeddings_without_moderation(texts, source, **kwargs)
    return embeddings, moderation_results


def get_embeddings(
    texts: List[str],
    source: Optional[str] = None,
    **kwargs,
) -> Tuple[List[List[float] | None], List[ModerationInfoType]]:
    """
    Obtain embeddings for the provided texts, replacing the embeddings of flagged texts with `None`.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - source (Optional[str], optional): Source identifier to potentially adjust embedding bias. Defaults to None.
    - **kwargs: Additional keyword arguments passed to the embedding function.

    Returns:
    - Tuple[List[Optional[List[float]]], ModerationInfoListType]: Tuple containing the list of embeddings (with None for flagged texts) and the moderation results.
    """
    assert len(texts) <= 2048, "The batch size should not be larger than 2048."
    assert all(texts), "No empty strings allowed in the input list."

    # replace newlines, which can negatively affect performance
    texts = [text.replace("\n", " ") for text in texts]

    # Check all texts for moderation flags
    moderation_results = moderation_check(texts)
    flags = [result["flagged"] for result in moderation_results]

    non_flagged_texts = [text for text, flag in zip(texts, flags) if not flag]
    non_flagged_embeddings = get_embeddings_without_moderation(non_flagged_texts, source, **kwargs)
    embeddings = [None if flag else non_flagged_embeddings.pop(0) for flag in flags]
    return embeddings, moderation_results


def get_embedding(
    text: str, source: Optional[str] = None, **kwargs
) -> Tuple[List[float] | None, ModerationInfoType]:
    """Obtain an embedding for a single text."""
    embedding, moderation_result = get_embeddings([text], source, **kwargs)
    return embedding[0], moderation_result[0]
