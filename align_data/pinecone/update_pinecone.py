import os
from typing import Dict, List, Union
import numpy as np
import openai

from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter
from align_data.pinecone.pinecone_db_handler import PineconeDB

from align_data.settings import USE_OPENAI_EMBEDDINGS, OPENAI_EMBEDDINGS_MODEL, \
    OPENAI_EMBEDDINGS_DIMS, OPENAI_EMBEDDINGS_RATE_LIMIT, \
    SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL, SENTENCE_TRANSFORMER_EMBEDDINGS_DIMS, \
    CHUNK_SIZE, MAX_NUM_AUTHORS_IN_SIGNATURE, EMBEDDING_LENGTH_BIAS

import logging
logger = logging.getLogger(__name__)


class ARDUpdater:
    def __init__(
        self, 
        min_chunk_size: int = ParagraphSentenceUnitTextSplitter.DEFAULT_MIN_CHUNK_SIZE,
        max_chunk_size: int = ParagraphSentenceUnitTextSplitter.DEFAULT_MAX_CHUNK_SIZE,
    ):
        self.text_splitter = ParagraphSentenceUnitTextSplitter(
            min_chunk_size=min_chunk_size,
            max_chunk_size=max_chunk_size,
        )

        self.pinecone_db = PineconeDB()
        
        if USE_OPENAI_EMBEDDINGS:
            import openai
            openai.api_key = os.environ['OPENAI_API_KEY']
        else:
            import torch
            from langchain.embeddings import HuggingFaceEmbeddings
            
            self.hf_embeddings = HuggingFaceEmbeddings(
                model_name=SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL,
                model_kwargs={'device': "cuda" if torch.cuda.is_available() else "cpu"},
                encode_kwargs={'show_progress_bar': False}
            )

    def update(self, custom_sources: List[str] = ['all']):
        """
        Update the given sources. If no sources are provided, updates all sources.

        :param custom_sources: List of sources to update.
        """

        for source in custom_sources:
            self.update_source(source)

    def update_source(self, source: str):
        """
        Updates the entries from the given source.

        :param source: The name of the source to update.
        """

        logger.info(f"Updating {source} entries...")
        
        # TODO: loop through mysql entries and update the pinecone db

        logger.info(f"Successfully updated {source} entries.")
    
    def batchify(self, iterable):
        """
        Divides the iterable into batches of size ~CHUNK_SIZE.

        :param iterable: The iterable to divide into batches.
        :returns: A generator that yields batches from the iterable.
        """

        entries_batch = []
        chunks_batch = []
        chunks_ids_batch = []
        sources_batch = []

        for entry in iterable:
            chunks, chunks_ids = self.create_chunk_ids_and_authors(entry)

            entries_batch.append(entry)
            chunks_batch.extend(chunks)
            chunks_ids_batch.extend(chunks_ids)
            sources_batch.extend([entry['source']] * len(chunks))

            # If this batch is large enough, yield it and start a new one.
            if len(chunks_batch) >= CHUNK_SIZE:
                yield self._create_batch(entries_batch, chunks_batch, chunks_ids_batch, sources_batch)

                entries_batch = []
                chunks_batch = []
                chunks_ids_batch = []
                sources_batch = []

        # Yield any remaining items.
        if entries_batch:
            yield self._create_batch(entries_batch, chunks_batch, chunks_ids_batch, sources_batch)
    
    def create_chunk_ids_and_authors(self, entry):
        signature = f"Title: {entry['title']}, Author(s): {self.get_authors_str(entry['authors'])}"
        chunks = self.text_splitter.split_text(entry['text'])
        chunks = [f"- {signature}\n\n{chunk}" for chunk in chunks]
        chunks_ids = [f"{entry['id']}_{str(i).zfill(6)}" for i in range(len(chunks))]
        return chunks, chunks_ids

    def _create_batch(self, entries_batch, chunks_batch, chunks_ids_batch, sources_batch):
        return {'entries_batch': entries_batch, 'chunks_batch': chunks_batch, 'chunks_ids_batch': chunks_ids_batch, 'sources_batch': sources_batch}

    def is_sql_entry_upserted(self, entry):
        """Upserts an entry to the SQL database and returns the success status"""
        return self.sql_db.upsert_entry(entry)
    
    def extract_embeddings(self, chunks_batch, sources_batch):
        if USE_OPENAI_EMBEDDINGS:
            return self.get_openai_embeddings(chunks_batch, sources_batch)
        else:
            return np.array(self.hf_embeddings.embed_documents(chunks_batch, sources_batch))

    def reset_dbs(self):
        self.sql_db.create_tables(True)
        self.pinecone_db.create_index(True)
    
    @staticmethod
    def preprocess_and_validate(entry):
        """Preprocesses and validates the entry data"""
        try:
            ARDUpdater.validate_entry(entry)
            
            return {
                'id': entry['id'],
                'source': entry['source'],
                'title': entry['title'],
                'text': entry['text'],
                'url': entry['url'],
                'date_published': entry['date_published'],
                'authors': entry['authors']
            }
        except ValueError as e:
            logger.error(f"Entry validation failed: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def validate_entry(entry: Dict[str, Union[str, list]], char_len_lower_limit: int = 0):
        metadata_types = {
            'id': str,
            'source': str,
            'title': str,
            'text': str,
            'url': str,
            'date_published': str,
            'authors': list
        }

        for metadata_type, metadata_type_type in metadata_types.items():
            if not isinstance(entry.get(metadata_type), metadata_type_type):
                raise ValueError(f"Entry metadata '{metadata_type}' is not of type '{metadata_type_type}' or is missing.")
                
        if len(entry['text']) < char_len_lower_limit:
            raise ValueError(f"Entry text is too short (< {char_len_lower_limit} characters).")

    @staticmethod
    def is_valid_entry(entry):
        """Checks if the entry is valid"""
        return entry is not None

    @staticmethod
    def get_openai_embeddings(chunks, sources=''):
        embeddings = np.zeros((len(chunks), OPENAI_EMBEDDINGS_DIMS))
        
        openai_output = openai.Embedding.create(
            model=OPENAI_EMBEDDINGS_MODEL, 
            input=chunks
        )['data']
        
        for i, (embedding, source) in enumerate(zip(openai_output, sources)):
            bias = EMBEDDING_LENGTH_BIAS.get(source, 1.0)
            embeddings[i] = bias * np.array(embedding['embedding'])
        
        return embeddings

    @staticmethod
    def get_authors_str(authors_lst: List[str]) -> str:
        if authors_lst == []: return 'n/a'
        if len(authors_lst) == 1: return authors_lst[0]
        else:
            authors_lst = authors_lst[:MAX_NUM_AUTHORS_IN_SIGNATURE]
            authors_str = f"{', '.join(authors_lst[:-1])} and {authors_lst[-1]}"
        return authors_str