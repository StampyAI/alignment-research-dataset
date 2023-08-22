import logging
from typing import List, Tuple, Dict, Any, Optional
from functools import wraps

import openai
from langchain.embeddings import HuggingFaceEmbeddings
from openai.error import (
    RateLimitError,
    InvalidRequestError,
    APIConnectionError,
    AuthenticationError,
    PermissionError,
    ServiceUnavailableError,
    InvalidAPIType,
    SignatureVerificationError,
    APIError,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
    retry_if_exception,
)

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

RETRY_CONDITIONS = (
    retry_if_exception_type(RateLimitError)
    | retry_if_exception_type(APIError)
    | retry_if_exception(lambda e: "502" in str(e))
)

hf_embeddings = (
    HuggingFaceEmbeddings(
        model_name=SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
        model_kwargs={"device": DEVICE},
        encode_kwargs={"show_progress_bar": False},
    )
    if not USE_OPENAI_EMBEDDINGS
    else None
)

# --------------------
# TYPE DEFINITIONS
# --------------------

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
        retry=RETRY_CONDITIONS,
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
        except (
            InvalidRequestError,
            APIConnectionError,
            AuthenticationError,
            PermissionError,
            ServiceUnavailableError,
            InvalidAPIType,
            SignatureVerificationError,
        ) as e:
            logger.error(f"OpenAI Error encountered: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error encountered: {e}")
            raise

    return wrapper


# --------------------
# UTILITY FUNCTIONS
# --------------------


def get_recursive_type(obj):
    """Get the nested type of a given object."""

    if isinstance(obj, list):
        return f"List[{get_recursive_type(obj[0]) if obj else 'Any'}]"
    elif isinstance(obj, tuple):
        return f"Tuple[{', '.join(get_recursive_type(item) for item in obj)}]"
    elif isinstance(obj, dict):
        keys, values = next(iter(obj.items())) if obj else (Any, Any)
        return f"Dict[{get_recursive_type(keys)}, {get_recursive_type(values)}]"
    else:
        return type(obj).__name__


def get_recursive_length(obj):
    """Determine the nested length of a given object."""
    lengths = []

    if isinstance(obj, (str, int, float)):
        return len(obj) if isinstance(obj, str) else 1
    if isinstance(obj, (list, tuple)):
        lengths.append(len(obj))
        sub_lengths = [get_recursive_length(item) for item in obj]

        # Flatten the sub_lengths list to a single list with the lengths of each level
        max_depth = max(len(sub) if isinstance(sub, list) else 0 for sub in sub_lengths)
        for i in range(max_depth):
            lengths.append(
                [sub[i] if isinstance(sub, list) and len(sub) > i else 1 for sub in sub_lengths]
            )
        return lengths
    if isinstance(obj, dict):
        lengths.append(len(obj))
        keys_lengths = [get_recursive_length(k) for k in obj.keys()]
        values_lengths = [get_recursive_length(v) for v in obj.values()]

        # Combine the lengths of keys and values for each level
        max_depth = max(len(sub) for sub in keys_lengths + values_lengths)
        for i in range(max_depth):
            lengths.append(
                [
                    keys_lengths[j][i] + values_lengths[j][i]
                    if len(keys_lengths[j]) > i and len(values_lengths[j]) > i
                    else 1
                    for j in range(len(obj))
                ]
            )
        return lengths
    return 1


# --------------------
# MAIN FUNCTIONS
# --------------------


@handle_openai_errors
def moderation_check(texts):
    return openai.Moderation.create(input=texts)["results"]


@handle_openai_errors
def compute_openai_embeddings(non_flagged_texts, engine, **kwargs):
    data = openai.Embedding.create(input=non_flagged_texts, engine=engine, **kwargs).data
    return [d["embedding"] for d in data]


def get_embeddings_without_moderation(
    texts: List[str],
    source: str = "no source",
    engine=OPENAI_EMBEDDINGS_MODEL,
    **kwargs,
) -> List[List[float]]:
    """
    Obtain embeddings without moderation checks.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - source (str, optional): Source identifier to potentially adjust embedding bias. Defaults to "no source".
    - engine (str, optional): Embedding engine to use (relevant for OpenAI). Defaults to OPENAI_EMBEDDINGS_MODEL.
    - **kwargs: Additional keyword arguments passed to the embedding function.

    Returns:
    - List[List[float]]: List of embeddings for the provided texts.
    """

    embeddings = []
    if texts:  # Only call the embedding function if there are non-flagged texts
        if USE_OPENAI_EMBEDDINGS:
            embeddings = compute_openai_embeddings(texts, engine, **kwargs)
        else:
            embeddings = hf_embeddings.embed_documents(texts)

    # Bias adjustment
    if bias := EMBEDDING_LENGTH_BIAS.get(source, 1.0):
        embeddings = [[bias * e for e in embedding] for embedding in embeddings]

    return embeddings


def get_embeddings_or_none_if_flagged(
    texts: List[str],
    source: str = "no source",
    engine=OPENAI_EMBEDDINGS_MODEL,
    **kwargs,
) -> Tuple[Optional[List[List[float]]], List[ModerationInfoType]]:
    """
    Obtain embeddings for the provided texts. If any text is flagged during moderation,
    the function returns None for the embeddings while still providing the moderation results.

    Parameters:
    - texts (List[str]): List of texts to be embedded.

    Returns:
    - Tuple[Optional[List[List[float]]], ModerationInfoListType]: Tuple containing the list of embeddings (or None if any text is flagged) and the moderation results.
    """
    moderation_results = moderation_check(texts)
    if any(result["flagged"] for result in moderation_results):
        return None, moderation_results

    embeddings = get_embeddings_without_moderation(texts, source, engine, **kwargs)
    return embeddings, moderation_results


def get_embeddings(
    texts: List[str],
    source: str = "no source",
    engine=OPENAI_EMBEDDINGS_MODEL,
    **kwargs,
) -> Tuple[List[Optional[List[float]]], List[ModerationInfoType]]:
    """
    Obtain embeddings for the provided texts, replacing the embeddings of flagged texts with `None`.

    Parameters:
    - texts (List[str]): List of texts to be embedded.
    - source (str, optional): Source identifier to potentially adjust embedding bias. Defaults to "no source".
    - engine (str, optional): Embedding engine to use (relevant for OpenAI). Defaults to OPENAI_EMBEDDINGS_MODEL.
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
    flagged_bools = [result["flagged"] for result in moderation_results]

    non_flagged_texts = [text for text, flagged in zip(texts, flagged_bools) if not flagged]
    non_flagged_embeddings = get_embeddings_without_moderation(
        non_flagged_texts, source, engine, **kwargs
    )

    # Reconstruct the final list of embeddings with None for flagged texts
    embeddings_iter = iter(non_flagged_embeddings)
    embeddings = [None if flagged else next(embeddings_iter) for flagged in flagged_bools]

    return embeddings, moderation_results


def get_embedding(
    text: str, source: str = "no source", engine=OPENAI_EMBEDDINGS_MODEL, **kwargs
) -> Tuple[List[float], ModerationInfoType]:
    """Obtain an embedding for a single text."""
    embedding, moderation_result = get_embeddings([text], source=source, engine=engine, **kwargs)
    return embedding[0], moderation_result[0]
