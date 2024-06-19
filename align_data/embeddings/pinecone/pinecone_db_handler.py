# dataset/pinecone_db_handler.py
import time
import logging
from typing import List, Tuple

from pinecone import Pinecone
from pinecone.core.client.models import ScoredVector
from urllib3.exceptions import ProtocolError

from align_data.embeddings.embedding_utils import get_embedding
from align_data.embeddings.pinecone.pinecone_models import (
    PineconeEntry,
    PineconeMetadata,
)
from align_data.settings import (
    PINECONE_INDEX_NAME,
    PINECONE_VALUES_DIMS,
    PINECONE_METRIC,
    PINECONE_API_KEY,
    PINECONE_ENVIRONMENT,
    PINECONE_NAMESPACE,
)

logger = logging.getLogger(__name__)


def with_retry(n=3, exceptions=(Exception,)):
    def retrier_wrapper(f):
        def wrapper(*args, **kwargs):
            for i in range(n):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    logger.error(f'Got exception while retrying: {e}')
                except Exception as e:
                    breakpoint()
                time.sleep(2 ** i)
            raise TimeoutError(f'Gave up after {n} tries')
        return wrapper
    return retrier_wrapper


class PineconeDB:
    def __init__(
        self,
        index_name: str = PINECONE_INDEX_NAME,
        values_dims: int = PINECONE_VALUES_DIMS,
        metric: str = PINECONE_METRIC,
        create_index: bool = False,
        log_index_stats: bool = False,
    ):
        self.index_name = index_name
        self.values_dims = values_dims
        self.metric = metric

        self.pinecone = Pinecone(
            api_key=PINECONE_API_KEY,
            environment=PINECONE_ENVIRONMENT,
        )

        if create_index:
            self.create_index()

        self.index = self.pinecone.Index(self.index_name)

        if log_index_stats:
            index_stats_response = self.index.describe_index_stats()
            logger.info(f"{self.index_name}:\n{index_stats_response}")

    @with_retry(exceptions=(ProtocolError,))
    def _get_vectors(self, entry):
        return entry.create_pinecone_vectors()

    @with_retry(exceptions=(ProtocolError,))
    def _upsert(self, vectors, upsert_size: int = 100, show_progress: bool = True):
        self.index.upsert(
            vectors=vectors,
            batch_size=upsert_size,
            namespace=PINECONE_NAMESPACE,
            show_progress=show_progress,
        )

    def upsert_entry(
        self, pinecone_entry: PineconeEntry, upsert_size: int = 100, show_progress: bool = True
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
    ) -> List[ScoredVector]:
        assert not isinstance(
            query, str
        ), "query must be a list of floats. Use query_PineconeDB_text for text queries"

        query_response = self.index.query(
            vector=query,
            top_k=top_k,
            include_values=include_values,
            include_metadata=include_metadata,
            **kwargs,
            namespace=PINECONE_NAMESPACE,
        )

        return [
            ScoredVector(
                id=match["id"],
                score=match["score"],
                metadata=PineconeMetadata(**match["metadata"]),
            )
            for match in query_response["matches"]
        ]

    def query_text(
        self,
        query: str,
        top_k: int = 10,
        include_values: bool = False,
        include_metadata: bool = True,
        **kwargs,
    ) -> List[ScoredVector]:
        query_vector = get_embedding(query)[0]
        if query_vector is None:
            print("The query is invalid.")
            return []

        return self.query_vector(
            query=query_vector,
            top_k=top_k,
            include_values=include_values,
            include_metadata=include_metadata,
            **kwargs,
        )

    def _find_items(self, ids):
        @with_retry()
        def get_item(id_):
            return list(self.index.list(prefix=id_, namespace=PINECONE_NAMESPACE))

        return [i for id_ in ids for i in get_item(id_)]

    @with_retry()
    def _del_items(self, ids):
        self.index.delete(ids=ids, namespace=PINECONE_NAMESPACE)

    @with_retry()
    def delete_entries(self, ids):
        items = self._find_items(ids)
        if items:
            self._del_items(items)

    def create_index(self, replace_current_index: bool = True):
        if replace_current_index:
            self.delete_index()

        self.pinecone.create_index(
            name=self.index_name,
            dimension=self.values_dims,
            metric=self.metric,
            metadata_config={"indexed": list(PineconeMetadata.__annotations__.keys())},
        )

    def delete_index(self):
        if self.index_name in self.pinecone.list_indexes():
            logger.info(f"Deleting index '{self.index_name}'.")
            self.pinecone.delete_index(self.index_name)

    def get_embeddings_by_ids(self, ids: List[str]) -> List[Tuple[str, List[float] | None]]:
        """
        Fetch embeddings for given entry IDs from Pinecone.

        Args:
        - ids (List[str]): List of entry IDs for which embeddings are to be fetched.

        Returns:
        - List[Tuple[str, List[float] | None]]: List of tuples containing ID and its corresponding embedding.
        """
        # TODO: check that this still works
        vectors = self.index.fetch(
            ids=ids,
            namespace=PINECONE_NAMESPACE,
        )["vectors"]
        return [(id, vectors.get(id, {}).get("values", None)) for id in ids]


def strip_block(text: str) -> str:
    return "\n".join(text.split("\n")[1:])
