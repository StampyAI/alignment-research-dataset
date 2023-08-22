import logging
from typing import List, Tuple, Dict, Union, Any, Optional
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

logger = logging.getLogger(__name__)

# Configuration for embeddings
hf_embeddings = (
    HuggingFaceEmbeddings(
        model_name=SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
        model_kwargs={"device": DEVICE},
        encode_kwargs={"show_progress_bar": False},
    )
    if not USE_OPENAI_EMBEDDINGS
    else None
)

# Type definitions
EmbeddingType = Optional[List[float]]  # Represents a single embedding or None
EmbeddingsListType = Optional[List[EmbeddingType]]  # List of embeddings
ModerationInfoListType = List[Dict[str, Any]]  # Moderation results for all input texts
GetEmbeddingsReturnType = Union[
    EmbeddingsListType, Tuple[EmbeddingsListType, ModerationInfoListType]
]  # Return type for main function

RETRY_CONDITIONS = (
    retry_if_exception_type(RateLimitError)
    |retry_if_exception_type(APIError)
    |retry_if_exception(lambda e: "502" in str(e))
)


def handle_openai_errors(func):
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
                logger.warning(
                    f"OpenAI 502 Bad Gateway error. Trying again. Error: {e}"
                )
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


@handle_openai_errors
def moderation_check(texts):
    return openai.Moderation.create(input=texts)["results"]


@handle_openai_errors
def compute_openai_embeddings(non_flagged_texts, engine, **kwargs):
    data = openai.Embedding.create(
        input=non_flagged_texts, engine=engine, **kwargs
    ).data
    return [d["embedding"] for d in data]


def get_embeddings(
    texts: List[str],
    embed_all: bool = False,
    return_moderation_info: bool = False,
    source: str = "no source",
    engine=OPENAI_EMBEDDINGS_MODEL,
    **kwargs,
) -> GetEmbeddingsReturnType:
    assert len(texts) <= 2048, "The batch size should not be larger than 2048."
    assert all(texts), "No empty strings allowed in the input list."

    # Step 1: Check all texts for moderation flags
    moderation_results = moderation_check(texts)
    flagged_bools = [result["flagged"] for result in moderation_results]

    # If not embedding all and any text is flagged, return None immediately
    if not embed_all and any(flagged_bools):
        if return_moderation_info:
            return None, moderation_results
        return None

    # Filter out flagged texts before embedding and replace newlines, which can negatively affect performance.
    non_flagged_texts = [
        text for text, flagged in zip(texts, flagged_bools) if not flagged
    ]
    non_flagged_texts = [text.replace("\n", " ") for text in non_flagged_texts]

    # Step 2: Compute embeddings for non-flagged texts
    non_flagged_embeddings = []
    if (
        non_flagged_texts
    ):  # Only call the embedding function if there are non-flagged texts
        if USE_OPENAI_EMBEDDINGS:
            non_flagged_embeddings = compute_openai_embeddings(
                non_flagged_texts, engine, **kwargs
            )
        else:
            non_flagged_embeddings = hf_embeddings.embed_documents(
                non_flagged_texts
            )  # Assuming this doesn't require the same error handling

    # Bias adjustment
    if bias := EMBEDDING_LENGTH_BIAS.get(source, 1.0):
        non_flagged_embeddings = [
            [bias * e for e in embedding] for embedding in non_flagged_embeddings
        ]

    # Step 3: Reconstruct the final list of embeddings with None for flagged texts
    non_flagged_iter = iter(non_flagged_embeddings)
    final_embeddings = [
        None if flagged else next(non_flagged_iter) for flagged in flagged_bools
    ]

    if return_moderation_info:
        return final_embeddings, moderation_results
    return final_embeddings


def get_embedding(text: str, **kwargs) -> List[float]:
    return get_embeddings(texts=[text], **kwargs)[0]


def get_recursive_type(obj):
    if isinstance(obj, list):
        # If the list is empty, just return List[Any]
        if not obj:
            return "List[Any]"
        # Recursively get the type of the first item in the list
        # This assumes that all items in the list are of the same type
        return f"List[{get_recursive_type(obj[0])}]"

    elif isinstance(obj, tuple):
        # Get the type of each item in the tuple
        types = ", ".join(get_recursive_type(item) for item in obj)
        return f"Tuple[{types}]"

    elif isinstance(obj, dict):
        # If the dict is empty, just return Dict[Any, Any]
        if not obj:
            return "Dict[Any, Any]"
        # Get the type of the first key and the first value in the dict
        # This assumes that all keys are of the same type and all values are of the same type
        key_type = get_recursive_type(next(iter(obj.keys())))
        value_type = get_recursive_type(next(iter(obj.values())))
        return f"Dict[{key_type}, {value_type}]"

    else:
        return type(obj).__name__


def get_recursive_length(obj):
    lengths = []

    # Base case: if the object is a string or number, return its length or 1
    if isinstance(obj, (str, int, float)):
        return len(obj) if isinstance(obj, str) else 1

    # If it's a list or tuple, get the length of the current level
    if isinstance(obj, (list, tuple)):
        lengths.append(len(obj))
        sub_lengths = [get_recursive_length(item) for item in obj]

        # Flatten the sub_lengths list to a single list with the lengths of each level
        max_depth = max(len(sub) if isinstance(sub, list) else 0 for sub in sub_lengths)
        for i in range(max_depth):
            lengths.append(
                [
                    sub[i] if isinstance(sub, list) and len(sub) > i else 1
                    for sub in sub_lengths
                ]
            )
        return lengths

    # If it's a dictionary, get the length of the current level and then of each key and value
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

    # For other types, just return 1
    return 1
