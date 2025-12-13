from typing import List, Literal, TypedDict

from pydantic import BaseModel
from align_data.embeddings.embedding_utils import Embedding


class MissingFieldsError(Exception):
    pass


class MissingEmbeddingModelError(Exception):
    pass


class PineconeMetadata(TypedDict):
    hash_id: str
    source: str
    title: str
    url: str
    date_published: float
    authors: List[str]
    text: str
    confidence: float | None
    miri_confidence: float | None
    miri_distance: Literal["core", "wider", "general"]
    needs_tech: bool | None
    # New fields for filtering
    tags: List[str]
    karma: int | None
    votes: int | None
    comment_count: int | None
    source_type: str | None
    # Per-chunk position fields (computed from chunk text)
    chunk_index: int
    chunk_count: int
    doc_words: int  # total words in document (sum of all chunk words)
    words_before: int
    words_after: int
    section_heading: str | None


class PineconeEntry(BaseModel):
    hash_id: str
    source: str
    title: str
    url: str
    date_published: float
    authors: List[str]
    confidence: float | None
    miri_confidence: float | None
    miri_distance: Literal["core", "wider", "general"]
    needs_tech: bool | None # whether ingestion fixes are needed
    tags: List[str]
    karma: int | None
    votes: int | None
    comment_count: int | None
    source_type: str | None
    # Embeddings
    embeddings: List[Embedding]

    def __init__(self, **data):
        """Check for missing (falsy) fields before initializing."""
        missing_fields = [
            field for field, value in data.items() if not str(value).strip()
        ]

        if missing_fields:
            raise MissingFieldsError(f"Missing fields: {missing_fields}")

        super().__init__(**data)

    def __repr__(self):
        def make_small(chunk: str) -> str:
            return (chunk[:45] + " [...] " + chunk[-45:]) if len(chunk) > 100 else chunk

        def display_chunks(chunks_lst: List[Embedding]) -> str:
            chunks = ", ".join(f'"{make_small(chunk.text)}"' for chunk in chunks_lst)
            return (
                f"[{chunks[:450]} [...] {chunks[-450:]} ]"
                if len(chunks) > 1000
                else f"[{chunks}]"
            )

        return f"PineconeEntry(hash_id={self.hash_id!r}, source={self.source!r}, title={self.title!r}, url={self.url!r}, date_published={self.date_published!r}, authors={self.authors!r}, text_chunks={display_chunks(self.embeddings)})"

    def create_pinecone_vectors(self) -> List[dict]:
        # Precompute word counts for position metadata
        word_counts = [len(e.text.split()) for e in self.embeddings]
        doc_words = sum(word_counts)
        chunk_count = len(self.embeddings)

        vectors = []
        words_so_far = 0
        for idx, embedding in enumerate(self.embeddings):
            chunk_words = word_counts[idx]
            words_before = words_so_far
            words_after = doc_words - words_before - chunk_words

            vectors.append({
                "id": f"{self.hash_id}_{hash(embedding.text)}",
                "values": embedding.vector,
                "metadata": {
                    key: value
                    for key, value in PineconeMetadata(
                        hash_id=self.hash_id,
                        source=self.source,
                        title=self.title,
                        authors=self.authors,
                        url=self.url,
                        date_published=self.date_published,
                        # Pinecone only accepts up to 40960 bytes in the metadata.
                        # Make sure to truncate the text if its too long.
                        text=embedding.text and embedding.text.strip()[:38000],
                        confidence=self.confidence,
                        miri_confidence=self.miri_confidence,
                        miri_distance=self.miri_distance,
                        needs_tech=self.needs_tech,
                        tags=self.tags,
                        karma=self.karma,
                        votes=self.votes,
                        comment_count=self.comment_count,
                        source_type=self.source_type,
                        # Per-chunk position fields
                        chunk_index=idx,
                        chunk_count=chunk_count,
                        doc_words=doc_words,
                        words_before=words_before,
                        words_after=words_after,
                        section_heading=embedding.section_heading,
                    ).items()
                    if value is not None  # Filter out keys with None values
                },
            })
            words_so_far += chunk_words

        return vectors
