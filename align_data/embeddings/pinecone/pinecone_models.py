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
    needs_tech: bool | None
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

    @property
    def chunk_num(self) -> int:
        return len(self.embeddings)

    def create_pinecone_vectors(self) -> List[dict]:
        return [
            {
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
                        text=embedding.text,
                        confidence=self.confidence,
                        miri_confidence=self.miri_confidence,
                        miri_distance=self.miri_distance,
                        needs_tech=self.needs_tech,
                    ).items()
                    if value is not None  # Filter out keys with None values
                },
            }
            for embedding in self.embeddings
        ]
