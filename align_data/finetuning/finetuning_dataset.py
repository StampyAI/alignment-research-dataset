import math
import random
from typing import List, Tuple, Generator

from torch.utils.data import IterableDataset, get_worker_info

from align_data.pinecone.pinecone_db_handler import PineconeDB
from align_data.pinecone.text_splitter import ParagraphSentenceUnitTextSplitter
from align_data.pinecone.update_pinecone import LengthFunctionType, TruncateFunctionType
from align_data.db.models import Article
from align_data.db.session import make_session


class FinetuningDataset(IterableDataset):
    def __init__(
        self,
        min_chunk_size: int = ParagraphSentenceUnitTextSplitter.DEFAULT_MIN_CHUNK_SIZE,
        max_chunk_size: int = ParagraphSentenceUnitTextSplitter.DEFAULT_MAX_CHUNK_SIZE,
        length_function: LengthFunctionType = ParagraphSentenceUnitTextSplitter.DEFAULT_LENGTH_FUNCTION,
        truncate_function: TruncateFunctionType = ParagraphSentenceUnitTextSplitter.DEFAULT_TRUNCATE_FUNCTION,
    ):
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.length_function = length_function
        self.truncate_function = truncate_function

        self.pinecone_db = PineconeDB()
        self.text_splitter = ParagraphSentenceUnitTextSplitter(
            min_chunk_size=self.min_chunk_size,
            max_chunk_size=self.max_chunk_size,
            length_function=self.length_function,
            truncate_function=self.truncate_function,
        )

        # Initialize the total count of articles
        with make_session() as session:
            self.total_articles = session.query(Article).count()

    def __iter__(self):
        worker_info = get_worker_info()
        if worker_info is None:  # Single-process loading
            return self._generate_pairs()
        else:  # Multi-process loading
            per_worker = math.ceil(self.total_articles / worker_info.num_workers)
            start = worker_info.id * per_worker
            end = min(start + per_worker, self.total_articles)
            return self._generate_pairs(start, end)

    def _fetch_random_article(self, session, exclude_id=None):
        """Fetch a random article excluding an optional article id."""
        random_id = random.randint(1, self.total_articles)
        while random_id == exclude_id:
            random_id = random.randint(1, self.total_articles)
        return session.query(Article).get(random_id)
    
    def _generate_pairs(self, start=0, end=None):
        end = end or self.total_articles

        with make_session() as session:
            while start < end:
                if random.random() < 0.5:
                    # Positive pairs
                    article = self._fetch_random_article(session)
                    chunks = self._get_random_chunks(article.text)
                    if chunks:
                        # For positive pairs, both text chunks are from the same article
                        text1 = chunks[0]
                        text2 = chunks[1]
                        label = 1
                else:
                    # Negative pairs
                    article1, article2 = self._fetch_two_distinct_articles(session)
                    chunk1 = self._get_random_chunks(article1.text, 1)
                    chunk2 = self._get_random_chunks(article2.text, 1)
                    if chunk1 and chunk2:
                        text1 = chunk1[0]
                        text2 = chunk2[0]
                        label = 0

                # Fetch embeddings from Pinecone if available, else compute them
                text1_embedding = self._get_embedding_from_pinecone(text1) or get_embeddings(text1)
                text2_embedding = self._get_embedding_from_pinecone(text2) or get_embeddings(text2)

                yield text1_embedding, text2_embedding, label
                start += 1

    def _fetch_two_distinct_articles(self, session):
        article1 = self._fetch_random_article(session)
        article2 = self._fetch_random_article(session, exclude_id=article1.id)
        return article1, article2

    def _get_random_chunks(self, text: str, num_chunks: int = 2) -> List[str]:
        """Return a number of random paragraphs from an article's text."""
        chunks = self.text_splitter.split_text(text)
        if len(chunks) < num_chunks:
            return None
        return random.sample(chunks, num_chunks)
    
    def _get_embedding_from_pinecone(self, text_chunk):
        # Here, you would check if the embedding for the text_chunk exists in Pinecone.
        # If it does, fetch and return the embedding.
        # If not, return None.
        # NOTE: The actual implementation might depend on the structure of your Pinecone database.
        ...
        return embedding or None
