# dataset/pinecone_db_handler.py

from typing import Dict
import pinecone

from align_data.settings import PINECONE_INDEX_NAME, PINECONE_VALUES_DIMS, PINECONE_METRIC, PINECONE_METADATA_ENTRIES, PINECONE_API_KEY, PINECONE_ENVIRONMENT

import logging
logger = logging.getLogger(__name__)


class PineconeDB:
    def __init__(
        self,
        index_name: str = PINECONE_INDEX_NAME,
        values_dims: int = PINECONE_VALUES_DIMS,
        metric: str = PINECONE_METRIC,
        metadata_entries: list = PINECONE_METADATA_ENTRIES,
        create_index: bool = False,
        log_index_stats: bool = True,
    ):
        self.index_name = index_name
        self.values_dims = values_dims
        self.metric = metric
        self.metadata_entries = metadata_entries
        
        pinecone.init(
            api_key = PINECONE_API_KEY,
            environment = PINECONE_ENVIRONMENT,
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
                    [f"{entry['id']}_{str(i).zfill(6)}" for i in range(len(entry['text_chunks']))], 
                    entry['embeddings'].tolist(), 
                    [
                        {
                            'entry_id': entry['id'],
                            'source': entry['source'],
                            'title': entry['title'],
                            'authors': entry['authors'],
                            'text': text_chunk,
                        } for text_chunk in entry['text_chunks']
                    ]
                )
            ),
            batch_size=upsert_size
        )
    
    def delete_entries(self, ids):
        self.index.delete(
            filter={"entry_id": {"$in": ids}}
        )

    def create_index(self, replace_current_index: bool = True):
        if replace_current_index:
            self.delete_index()
        
        pinecone.create_index(
            name=self.index_name,
            dimension=self.values_dims,
            metric=self.metric,
            metadata_config = {"indexed": self.metadata_entries},
        )

    def delete_index(self):
        if self.index_name in pinecone.list_indexes():
            logger.info(f"Deleting index '{self.index_name}'.")
            pinecone.delete_index(self.index_name)