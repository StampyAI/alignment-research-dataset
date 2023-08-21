import math
import random
from typing import List, Tuple, Generator
from collections import deque

import torch
from torch.utils.data import IterableDataset, get_worker_info
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import func

from align_data.db.session import make_session
from align_data.common.utils import get_embeddings
from align_data.pinecone.pinecone_db_handler import PineconeDB
from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter
from align_data.pinecone.update_pinecone import get_text_chunks
from align_data.db.models import Article


MAX_RETRIES = 3
RETRY_DELAY = 5


class FinetuningDataset(IterableDataset):
    def __init__(self, num_batches_per_epoch, cache_size=1280):
        self.num_batches_per_epoch = num_batches_per_epoch
        self.article_cache = deque(maxlen=cache_size)

        self.text_splitter = ParagraphSentenceUnitTextSplitter()
        self.pinecone_db = PineconeDB()
        
        with make_session() as session:
            self.all_article_ids = [row[0] for row in session.query(Article.id).all()]
            self.total_articles = len(self.all_article_ids)

    def __len__(self):
        return self.num_batches_per_epoch
    
    def __iter__(self):
        worker_info = get_worker_info()
        if worker_info is None:  # Single-process loading
            with make_session() as session:
                self.session = session
                return self._generate_pairs()
        else:  # Multi-process loading
            # Create a new session for this worker
            self.session = make_session(auto_commit=False).__enter__()
            per_worker = math.ceil(self.total_articles / worker_info.num_workers)
            start = worker_info.id * per_worker
            end = min(start + per_worker, self.total_articles)
            return self._generate_pairs(start, end)
    
    def _fetch_random_articles(self, batch_size=1) -> List[Article]:
        """Fetch a batch of random articles."""
        # If the list has fewer IDs than needed, raise an exception
        random_selected_ids = random.sample(self.all_article_ids, batch_size)
        return self.session.query(Article).filter(Article.id.in_(random_selected_ids)).all()
    
    def _get_random_chunks(self, article: Article, num_chunks: int = 2) -> List[Tuple[int, str]]:
        chunks = get_text_chunks(article, self.text_splitter)
        if len(chunks) < num_chunks:
            return None
        random_chunk_indices = random.sample(range(len(chunks)), num_chunks)
        random_chunks = [chunks[idx] for idx in random_chunk_indices]
        return list(zip(random_chunk_indices, random_chunks))

    def _get_embeddings(self, article: Article, chunks: List[Tuple[str, str]]) -> List[List[float]]:
        full_ids = [f"{article.id}_{str(idx).zfill(6)}" for idx, _ in chunks]
        _embeddings = self.pinecone_db.get_embeddings_by_ids(full_ids)
        
        embeddings = []
        for (chunk_idx, chunk), (full_id, embedding) in zip(chunks, _embeddings):
            if embedding is None:
                embedding = get_embeddings([chunk]).tolist()[0]
            embeddings.append(torch.tensor(embedding))

        return embeddings
    
    def _generate_pairs(self, start=0, end=None, neg_pos_proportion=0.5) -> Generator[Tuple[List[float], List[float], int], None, None]:
        end = end or self.total_articles

        batches_yielded = 0
        while start < end:
            start += 1
            if random.random() < neg_pos_proportion:
                # Positive pairs
                article = self._fetch_random_articles()[0]
                chunks = self._get_random_chunks(article, 2)
                embedding_1, embedding_2 = self._get_embeddings(article, chunks)
                label = 1
            else:
                # Negative pairs
                article1, article2 = self._fetch_random_articles(batch_size=2)
                chunk1 = self._get_random_chunks(article1, 1)
                chunk2 = self._get_random_chunks(article2, 1)
                embedding_1, embedding_2 = self._get_embeddings(article1, chunk1)[0], self._get_embeddings(article2, chunk2)[0]
                label = 0
            yield embedding_1.squeeze(), embedding_2.squeeze(), torch.tensor(label, dtype=torch.int64)
            batches_yielded += 1
            
            if self.num_batches_per_epoch and batches_yielded >= self.num_batches_per_epoch:
                break

        if get_worker_info() is not None:
            self.session.close()
