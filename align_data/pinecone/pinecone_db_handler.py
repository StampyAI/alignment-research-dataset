# dataset/pinecone_db_handler.py
import logging
from typing import Dict, List, Tuple, Union

import pinecone

from align_data.common.utils import get_embeddings, embed_query
from align_data.pinecone.pinecone_models import PineconeEntry, PineconeMatch, PineconeMetadata
from align_data.settings import (
    PINECONE_INDEX_NAME,
    PINECONE_VALUES_DIMS,
    PINECONE_METRIC,
    PINECONE_METADATA_KEYS,
    PINECONE_API_KEY,
    PINECONE_ENVIRONMENT,
    PINECONE_NAMESPACE
)


logger = logging.getLogger(__name__)


class PineconeDB:
    def __init__(
        self,
        index_name: str = PINECONE_INDEX_NAME,
        values_dims: int = PINECONE_VALUES_DIMS,
        metric: str = PINECONE_METRIC,
        metadata_keys: list = PINECONE_METADATA_KEYS,
        create_index: bool = False,
        log_index_stats: bool = False,
    ):
        self.index_name = index_name
        self.values_dims = values_dims
        self.metric = metric
        self.metadata_keys = metadata_keys

        pinecone.init(
            api_key=PINECONE_API_KEY,
            environment=PINECONE_ENVIRONMENT,
        )

        if create_index:
            self.create_index()

        self.index = pinecone.Index(index_name=self.index_name)

        if log_index_stats:
            index_stats_response = self.index.describe_index_stats()
            logger.info(f"{self.index_name}:\n{index_stats_response}")

    def upsert_entry(self, pinecone_entry: PineconeEntry, upsert_size: int = 100):
        vectors = pinecone_entry.create_pinecone_vectors()
        
        self.index.upsert(
            vectors=vectors,
            batch_size=upsert_size,
            namespace=PINECONE_NAMESPACE
        )

    def query_vector(
            self, query: List[float], top_k: int = 10, 
            include_values: bool = False, include_metadata: bool = True, **kwargs
            ) -> List[PineconeMatch]:
        assert not isinstance(query, str), "query must be a list of floats. Use query_PineconeDB_text for text queries"
        
        query_response = self.index.query(
            vector=query,
            top_k=top_k,
            include_values=include_values,
            include_metadata=include_metadata,
            **kwargs,
            namespace=PINECONE_NAMESPACE,
        )
        print(query_response)

        return query_response['matches']

    
    def query_text(
            self, query: str, top_k: int = 10,
            include_values: bool = False, include_metadata: bool = True, **kwargs
            ) -> List[PineconeMatch]:
        
        query_vector = embed_query(query)
        return self.query_vector(
            query=query_vector, top_k=top_k, 
            include_values=include_values, include_metadata=include_metadata, **kwargs
        )
    
        # def query
        #   vector: Optional[List[float]] = None,
        #   id: Optional[str] = None,
        #   queries: Optional[Union[List[QueryVector], List[Tuple]]] = None,
        #   top_k: Optional[int] = None,
        #   namespace: Optional[str] = None,
        #   filter: Optional[Dict[str, Union[str, float, int, bool, List, dict]]] = None,
        #   include_values: Optional[bool] = None,
        #   include_metadata: Optional[bool] = None,
        #   sparse_vector: Optional[Union[SparseValues, Dict[str, Union[List[float], List[int]]]]] = None,
        #   **kwargs

    def delete_entries(self, ids):
        self.index.delete(filter={"entry_id": {"$in": ids}})

    def create_index(self, replace_current_index: bool = True):
        if replace_current_index:
            self.delete_index()

        pinecone.create_index(
            name=self.index_name,
            dimension=self.values_dims,
            metric=self.metric,
            metadata_config={"indexed": self.metadata_keys},
        )

    def delete_index(self):
        if self.index_name in pinecone.list_indexes():
            logger.info(f"Deleting index '{self.index_name}'.")
            pinecone.delete_index(self.index_name)

    def get_embeddings_by_ids(self, ids: List[str]) -> List[Tuple[str, Union[List[float], None]]]:
        """
        Fetch embeddings for given entry IDs from Pinecone.

        Args:
        - ids (List[str]): List of entry IDs for which embeddings are to be fetched.

        Returns:
        - List[Tuple[str, Union[List[float], None]]]: List of tuples containing ID and its corresponding embedding.
        """
        #TODO: check that this still works
        vectors = self.index.fetch(
            ids=ids,
            namespace=PINECONE_NAMESPACE,
        )['vectors']
        return [(id, vectors.get(id, {}).get("values", None)) for id in ids]


def strip_block(text: str) -> str:
    return "\n".join(text.split("\n")[1:])