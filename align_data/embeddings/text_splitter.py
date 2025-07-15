import logging
from typing import Iterable, Any
import re

from align_data.settings import (
    DEFAULT_CHUNK_TOKENS,
    OVERLAP_TOKENS,
)

logger = logging.getLogger(__name__)


Vector = list[float]
Embedding = tuple[str, Vector, dict[str, Any]]


# Regex for sentence splitting
_SENT_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def approx_token_count(text: str) -> int:
    return len(text.split()) // 4


def yield_word_chunks(
    text: str, max_tokens: int = DEFAULT_CHUNK_TOKENS
) -> Iterable[str]:
    words = text.split()
    if not words:
        return

    current = ""
    for word in words:
        new_chunk = f"{current} {word}".strip()
        if current and approx_token_count(new_chunk) > max_tokens:
            yield current
            current = word
        else:
            current = new_chunk
    if current:  # Only yield non-empty final chunk
        yield current


def yield_spans(text: str, max_tokens: int = DEFAULT_CHUNK_TOKENS) -> Iterable[str]:
    """
    Yield text spans in priority order: paragraphs, sentences, words.
    Each span is guaranteed to be under max_tokens.

    Args:
        text: The text to split
        max_tokens: Maximum tokens per chunk

    Yields:
        Spans of text that fit within token limits
    """
    # Return early for empty text
    if not text.strip():
        return

    for paragraph in text.split("\n\n"):
        if not paragraph.strip():
            continue

        if approx_token_count(paragraph) <= max_tokens:
            yield paragraph
            continue

        for sentence in _SENT_SPLIT_RE.split(paragraph):
            if not sentence.strip():
                continue

            if approx_token_count(sentence) <= max_tokens:
                yield sentence
                continue

            for chunk in yield_word_chunks(sentence, max_tokens):
                yield chunk


def chunk_text(
    text: str, max_tokens: int = DEFAULT_CHUNK_TOKENS, overlap: int = OVERLAP_TOKENS
) -> Iterable[str]:
    """
    Split text into chunks respecting semantic boundaries while staying within token limits.

    Args:
        text: The text to chunk
        max_tokens: Maximum tokens per chunk (default: 512 for optimal semantic search)
        overlap: Number of tokens to overlap between chunks (default: 50)

    Returns:
        List of text chunks
    """
    text = text.strip()
    if not text:
        return

    if approx_token_count(text) <= max_tokens:
        yield text
        return

    overlap_chars = overlap * 4
    current = ""

    for span in yield_spans(text, max_tokens):
        current = f"{current} {span}".strip()
        if approx_token_count(current) < max_tokens:
            continue

        if overlap <= 0:
            yield current
            current = ""
            continue

        overlap_text = current[-overlap_chars:]
        clean_break = max(
            overlap_text.rfind(". "), overlap_text.rfind("! "), overlap_text.rfind("? ")
        )

        if clean_break < 0:
            yield current
            current = ""
            continue

        break_offset = -overlap_chars + clean_break + 1
        chunk = current[break_offset:].strip()
        yield current
        current = chunk

    if current:
        yield current.strip()


def split_text(text: str) -> list[str]:
    return list(chunk_text(text))
