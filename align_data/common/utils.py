import logging
from typing import List

import openai
from langchain.embeddings import HuggingFaceEmbeddings
from openai.error import (RateLimitError, InvalidRequestError, 
                          APIConnectionError, AuthenticationError, 
                          PermissionError, ServiceUnavailableError,
                          InvalidAPIType, SignatureVerificationError,
                          APIError)
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, retry_if_exception

from align_data.settings import (
    USE_OPENAI_EMBEDDINGS,
    OPENAI_EMBEDDINGS_MODEL,
    EMBEDDING_LENGTH_BIAS,
    SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
    DEVICE
)

logger = logging.getLogger(__name__)


hf_embeddings = HuggingFaceEmbeddings(
    model_name=SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
    model_kwargs={"device": DEVICE},
    encode_kwargs={"show_progress_bar": False},
) if not USE_OPENAI_EMBEDDINGS else None


@retry(
    wait=wait_random_exponential(multiplier=1, min=2, max=30), 
    stop=stop_after_attempt(6),
    retry=(retry_if_exception_type(RateLimitError) |
           retry_if_exception_type(APIError) |
           retry_if_exception(lambda e: '502' in str(e)))
)
def get_embeddings(
    list_of_text: List[str], 
    source: str = 'no source', 
    engine=OPENAI_EMBEDDINGS_MODEL, 
    **kwargs
    ) -> List[List[float]]:
    assert len(list_of_text) <= 2048, "The batch size should not be larger than 2048."

    list_of_text = [text.replace("\n", " ") for text in list_of_text]

    try:
        if USE_OPENAI_EMBEDDINGS:
            data = openai.Embedding.create(input=list_of_text, engine=engine, **kwargs).data
            embeddings = [d["embedding"] for d in data]
        else:
            embeddings = hf_embeddings.embed_documents(list_of_text)

        #TODO: figure out a good way to bias
        if bias := EMBEDDING_LENGTH_BIAS.get(source, 1.0):
            embeddings = [[bias * e for e in embedding] for embedding in embeddings]
        return embeddings

    except RateLimitError as e:
        logger.warning(f"OpenAI Rate limit error. Trying again. Error: {e}")
        raise

    except APIError as e:
        if '502' in str(e):
            logger.warning(f"OpenAI 502 Bad Gateway error. Trying again. Error: {e}")
        else:
            logger.error(f"OpenAI API Error encountered: {e}")
        raise

    except (InvalidRequestError, APIConnectionError, AuthenticationError,
            PermissionError, ServiceUnavailableError, InvalidAPIType,
            SignatureVerificationError) as e:
        logger.error(f"OpenAI Error encountered: {e}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error encountered: {e}")
        raise


def embed_query(query: str, engine=OPENAI_EMBEDDINGS_MODEL, **kwargs) -> List[float]:
    return get_embeddings([query], engine=engine, **kwargs)[0]


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
            lengths.append([sub[i] if isinstance(sub, list) and len(sub) > i else 1 for sub in sub_lengths])
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
                [keys_lengths[j][i] + values_lengths[j][i] if len(keys_lengths[j]) > i and len(values_lengths[j]) > i else 1
                 for j in range(len(obj))])
        return lengths

    # For other types, just return 1
    return 1
