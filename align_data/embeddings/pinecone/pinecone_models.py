from typing import List, TypedDict

from pydantic import BaseModel, validator


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


class PineconeEntry(BaseModel):
    hash_id: str
    source: str
    title: str
    url: str
    date_published: float
    authors: List[str]
    text_chunks: List[str]
    confidence: float | None
    embeddings: List[List[float] | None]

    def __init__(self, **data):
        """Check for missing (falsy) fields before initializing."""
        missing_fields = [field for field, value in data.items() if not str(value).strip()]

        if missing_fields:
            raise MissingFieldsError(f"Missing fields: {missing_fields}")

        super().__init__(**data)

    def __repr__(self):
        def make_small(chunk: str) -> str:
            return (chunk[:45] + " [...] " + chunk[-45:]) if len(chunk) > 100 else chunk

        def display_chunks(chunks_lst: List[str]) -> str:
            chunks = ", ".join(f'"{make_small(chunk)}"' for chunk in chunks_lst)
            return (
                f"[{chunks[:450]} [...] {chunks[-450:]} ]" if len(chunks) > 1000 else f"[{chunks}]"
            )

        return f"PineconeEntry(hash_id={self.hash_id!r}, source={self.source!r}, title={self.title!r}, url={self.url!r}, date_published={self.date_published!r}, authors={self.authors!r}, text_chunks={display_chunks(self.text_chunks)})"

    @property
    def chunk_num(self) -> int:
        return len(self.text_chunks)

    def create_pinecone_vectors(self) -> List[dict]:
        return [
            {
                'id': f"{self.hash_id}_{str(i).zfill(6)}",
                'values': self.embeddings[i],
                'metadata': {
                    key: value
                    for key, value in PineconeMetadata(
                        hash_id=self.hash_id,
                        source=self.source,
                        title=self.title,
                        authors=self.authors,
                        url=self.url,
                        date_published=self.date_published,
                        text=self.text_chunks[i],
                        confidence=self.confidence,
                    ).items()
                    if value is not None  # Filter out keys with None values
                },
            }
            for i in range(self.chunk_num)
            if self.embeddings[i]  # Skips flagged chunks
        ]
