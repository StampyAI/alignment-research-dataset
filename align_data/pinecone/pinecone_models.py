from typing import Dict, List, Optional, TypedDict, Tuple, Any
from pydantic import BaseModel, validator


class PineconeMetadata(TypedDict):
    entry_id: str
    source: str
    title: str
    authors: List[str]
    text: str
    url: str
    date_published: int


class PineconeMatch(TypedDict):
    id: str
    score: float
    values: Optional[List[float]]
    metadata: PineconeMetadata
    sparseValues: Optional[Dict[str, List[float]]]


# TODO: avoid having the model compute the vectors
class PineconeEntry(BaseModel):
    id: str
    source: str
    title: str
    url: str
    date_published: float
    authors: List[str]
    text_chunks: List[str]
    embeddings: List[List[float]]

    def __repr__(self):
        def make_small(chunk: str) -> str:
            return (chunk[:45] + " [...] " + chunk[-45:]) if len(chunk) > 100 else chunk

        def display_chunks(chunks: List[str]) -> str:
            chunks = ", ".join(f'"{make_small(chunk)}"' for chunk in chunks)
            return (
                f"[{chunks[:450]} [...] {chunks[-450:]} ]" if len(chunks) > 1000 else f"[{chunks}]"
            )

        return f"PineconeEntry(id={self.id!r}, source={self.source!r}, title={self.title!r}, url={self.url!r}, date_published={self.date_published!r}, authors={self.authors!r}, text_chunks={display_chunks(self.text_chunks)})"

    @validator(
        "id",
        "source",
        "title",
        "url",
        "date_published",
        "authors",
        "text_chunks",
        pre=True,
        always=True,
    )
    def empty_strings_not_allowed(cls, value):
        if not str(value).strip():
            raise ValueError("Attribute should not be empty.")
        return value

    @property
    def chunk_num(self) -> int:
        return len(self.text_chunks)

    def create_pinecone_vectors(self) -> List[Tuple[str, List[float], Dict[str, Any]]]:
        return [
            (
                f"{self.id}_{str(i).zfill(6)}",  # entry.id is the article's hash_id
                self.embeddings[i],  # values
                {
                    "entry_id": self.id,
                    "source": self.source,
                    "title": self.title,
                    "authors": self.authors,
                    "url": self.url,
                    "date_published": self.date_published,
                    "text": self.text_chunks[i],
                },  # metadata
            )
            for i in range(self.chunk_num)
        ]
