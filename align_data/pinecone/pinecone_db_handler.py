# dataset/pinecone_db_handler.py

import datetime
import logging
from typing import Dict, List, Tuple, Union
from dataclasses import dataclass

import pinecone

from align_data.common.utils import get_embeddings
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

    def upsert_entry(self, entry: Dict, upsert_size=100):
        self.index.upsert(
            vectors=list(
                zip(
                    [
                        f"{entry['id']}_{str(i).zfill(6)}"
                        for i in range(len(entry["text_chunks"]))
                    ],
                    entry["embeddings"].tolist(),
                    [
                        {
                            "entry_id": entry["id"],
                            "source": entry["source"],
                            "title": entry["title"],
                            "authors": entry["authors"],
                            "url": entry["url"],
                            "date_published": entry["date_published"],
                            "text": text_chunk,
                        }
                        for text_chunk in entry["text_chunks"]
                    ],
                )
            ),
            batch_size=upsert_size,
            namespace=PINECONE_NAMESPACE
        )
        
    def query(self, query: Union[str, List[float]], top_k=10, include_values=False, include_metadata=True, **kwargs):
        
        @dataclass
        class Block:
            id: str
            source: str
            title: str
            authors: str
            text: str
            url: str
            date_published: str

        if isinstance(query, str):
            query = list(get_embeddings(query)[0])
        
        query_response = self.index.query(
            vector=query,
            top_k=top_k,
            include_values=include_values,
            include_metadata=include_metadata,
            **kwargs,
            namespace=PINECONE_NAMESPACE,
        )
        print(query_response)
        
        blocks = []
        for match in query_response['matches']:

            date_published = match['metadata']['date_published']

            if type(date_published) == datetime.date: 
                date_published = date_published.strftime("%Y-%m-%d") # iso8601

            blocks.append(Block(
                id = match['id'],
                source = match['metadata']['source'],
                title = match['metadata']['title'],
                authors = match['metadata']['authors'],
                # text = strip_block(match['metadata']['text']),
                text = match['metadata']['text'],
                url = match['metadata']['url'],
                date_published = date_published,
            ))

        return blocks
    
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
        vectors = self.index.fetch(
            ids=ids,
            namespace=PINECONE_NAMESPACE,
        )['vectors']
        return [(id, vectors.get(id, {}).get("values", None)) for id in ids]


# we add the title and authors inside the contents of the block, so that
# searches for the title or author will be more likely to pull it up. This
# strips it back out.
# import re
# def strip_block(text: str) -> str:
#     r = re.match(r"^\"(.*)\"\s*-\s*Title:.*$", text, re.DOTALL)
#     if not r:
#         print("Warning: couldn't strip block")
#         print(text)
#     return r.group(1) if r else text

def strip_block(text: str) -> str:
    return "\n".join(text.split("\n")[1:])