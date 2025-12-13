import logging
import re
import tiktoken
from dataclasses import dataclass
from typing import List, Tuple, Dict, NamedTuple
from collections import defaultdict, deque

from align_data.settings import (
    MIN_CHUNK_LENGTH,
    MAX_CHUNK_LENGTH,
    PREFERRED_CHUNK_LENGTH,
    CHUNK_MAX_OVERLAP,
)

logger = logging.getLogger(__name__)


@dataclass
class BPat:
    name: str
    regexes: List[str]
    use_start: bool  # True=use match.start(), False=use match.end()
    priority: int | None = None


# Define boundary patterns in priority order (higher priority = better boundary = picked if there are any in range)
PATTERNS = [
    BPat(
        "hr",
        [r"(?<=\n\n)\s{0,3}(?P<char>[*.=+-])(?:\s*(?P=char))(?:\s*(?P=char))*\s*\n"],
        True,
    ),
    BPat("h1", [r"^# ", r"^.+\n={3,}", "<h1"], True),
    BPat("h2", [r"^## ", r"^.+\n-{3,}", "<h2"], True),
    BPat("h3", [r"^### ", "<h3"], True),
    BPat("h4", [r"^#### ", "<h4"], True),
    BPat(
        "h5",
        [
            r"^##### ",
            "<h5",
            r"(?<=\n\n)\s{0,2}(?P<bold>[*_])(?P=bold).+(?P=bold)(?P=bold)$\n\s*\n",
        ],
        True,
    ),
    BPat(
        "h6",
        [
            r"^###### ",
            "<h6",
            r"(?<=\n\n)\s{0,2}(?P<italic>[*_])(?!(?P=italic)).+(?<!(?P=italic))(?P=italic)\n\s*\n",
        ],
        True,
    ),
    BPat("para", [r"\n\s*\n"], False),  # End of paragraph
    BPat("sent", [r"[.!?]+\s+"], False),  # End of sentence
    BPat("word", [r"\s+"], False),  # End of word
]
TOKENIZER = tiktoken.get_encoding("cl100k_base")

# Assign priorities (higher = better boundary)
for idx, pat in enumerate(PATTERNS):
    pat.priority = len(PATTERNS) - 1 - idx


def extract_headings(doc: str) -> List[Tuple[int, int, str]]:
    """Extract headings with their positions and levels.

    Returns: [(position, level, text), ...] sorted by position.
    """
    headings = []

    # Match markdown # headings (# through ######)
    for match in re.finditer(r'^(#{1,6})\s+(.+?)$', doc, re.MULTILINE):
        level = len(match.group(1))
        text = match.group(2).strip()
        headings.append((match.start(), level, text))

    # Match underline-style headings (Title\n=== or Section\n---)
    for match in re.finditer(r'^(.+?)\n(={3,}|-{3,})$', doc, re.MULTILINE):
        level = 1 if match.group(2)[0] == '=' else 2
        text = match.group(1).strip()
        headings.append((match.start(), level, text))

    return sorted(headings, key=lambda h: h[0])


def heading_at_position(headings: List[Tuple[int, int, str]], pos: int) -> str | None:
    """Get the heading context at a given position.

    Returns heading hierarchy like "Methods > Data Collection" or None.
    """
    stack = []  # [(level, text), ...]

    for h_pos, level, text in headings:
        if h_pos > pos:
            break
        # Pop any headings at same or deeper level
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, text))

    if not stack:
        return None

    return " > ".join(text for _, text in stack)


class Boundary(NamedTuple):
    pos: int
    pattern: BPat


class Chunk(NamedTuple):
    start_pos: int
    value: str
    remain_chars: int  # will be presented as part of post-RAG prompt
    tokencount: int
    pat: BPat
    section_heading: str | None  # heading context like "Methods > Data Collection"


class TokenMapper:
    """Bidirectional token<->character mapping"""

    def __init__(self, doc: str):
        self.doc = doc
        self.tokens = TOKENIZER.encode(doc)
        self.token_strings = [TOKENIZER.decode([token]) for token in self.tokens]

        # Build char_to_token mapping
        self.char_to_token = []
        token_idx = 0
        char_pos = 0

        for token_idx, token_str in enumerate(self.token_strings):
            for _ in range(len(token_str)):
                if char_pos < len(doc):
                    self.char_to_token.append(token_idx)
                    char_pos += 1

        # Pad to doc length
        while len(self.char_to_token) < len(doc):
            self.char_to_token.append(len(self.tokens))

        # Build token_to_char mapping
        self.token_to_char = [0]
        char_pos = 0
        for token_str in self.token_strings:
            char_pos += len(token_str)
            self.token_to_char.append(min(char_pos, len(doc)))

    def char_span_to_tokens(self, start_char: int, end_char: int) -> int:
        """Convert character span to token count"""
        if start_char >= len(self.char_to_token) or end_char > len(self.char_to_token):
            return 0
        if start_char >= end_char:
            return 0

        start_token = self.char_to_token[start_char]
        end_token = self.char_to_token[end_char - 1]
        return end_token - start_token + 1

    def tokens_forward_from_char(self, start_char: int, token_count: int) -> int:
        """Given start_char, find end_char for approximately token_count tokens"""
        start_char = max(0, min(len(self.char_to_token) - 1, start_char))

        start_token = self.char_to_token[start_char]
        target_token = start_token + token_count

        if target_token >= len(self.token_to_char):
            return len(self.doc)
        if target_token < 0:
            return 0

        return self.token_to_char[target_token]


def detect_all_boundaries(doc: str) -> List[Boundary]:
    """Detect all possible boundaries using separate regexes"""
    boundaries = []

    for pattern in PATTERNS:
        for regex in pattern.regexes:
            for match in re.finditer(regex, doc, re.MULTILINE):
                pos = match.start() if pattern.use_start else match.end()
                if pos >= len(doc):
                    break
                boundaries.append(Boundary(pos, pattern))

    # Sort by position, then by priority (higher priority first for ties)
    return sorted(boundaries, key=lambda b: (b.pos, -b.pattern.priority))


def boundary_cost(
    boundary: Boundary, preferred_pos: int, doc_tokens: TokenMapper
) -> Tuple[int, ...]:
    """translate boundary distance to cost using ordinal ranking - more leading zeros = lower-sort-order tuple = better"""
    # Distance in tokens from preferred position
    boundary_tokens = doc_tokens.char_to_token[boundary.pos]
    preferred_tokens = doc_tokens.char_to_token[preferred_pos]
    distance = abs(boundary_tokens - preferred_tokens)

    # Create score tuple: higher priority patterns get later lower positions
    # eg, if we only had two patterns:
    # priority_0=0 -> cost_0[0]; cost_0 = (distance, 0)
    # priority_1=1 -> cost_1[1]; cost_1 = (0, distance)
    # cost_1 < cost_0
    cost = [0] * len(PATTERNS)
    cost[boundary.pattern.priority] = distance + 1
    return tuple(cost)


def chunks(
    doc: str,
    min_chunk_length: int = 700,
    max_chunk_length: int = 1500,
    preferred_length_tokens: int = 1000,
    chunk_max_overlap: int = 100,
) -> Tuple[List[Chunk], Dict[str, int]]:
    """
    Chunk document respecting token constraints and semantic boundaries.

    Returns: (chunks_list, boundary_stats)
    where chunks_list contains (chunk_text, chars_before, chars_after)
    """
    # Strip non-semantic garbage (URLs are kept - they're semantic)
    doc = re.sub(r"(?:\s*\n){4,}", "\n\n", doc)  # collapse blank lines
    doc = re.sub(r'data:[a-zA-Z0-9/;,=+-]+;base64,[A-Za-z0-9+/=\s]+', '[data-uri]', doc)
    doc = re.sub(r"'{4,}", "'", doc)  # apostrophe corruption
    doc = re.sub(r'(?<![:/\w])[A-Za-z0-9+/]{80,}={0,2}', '[base64]', doc)  # raw base64
    doc = re.sub(r'(?:^|["\s])([MLHVCSQTAZmlhvcsqtaz][0-9,.\s-]{100,})(?:["\s]|$)', ' [svg] ', doc)
    doc = (
        doc
        .replace("<|endofprompt|>", "<endofprompt>")
        .replace("<|endoftext|>", "<endoftext>")
    )

    if not doc.strip():
        return [], {}

    # Extract headings for section context (before tokenization)
    headings = extract_headings(doc)

    doc_tokens = TokenMapper(doc)
    assert len(doc_tokens.char_to_token) == len(doc)

    # Verify constraint sanity
    assert max_chunk_length >= 2 * min_chunk_length, (
        f"error: max_chunk_length ({max_chunk_length}) should be at least 2x min_chunk_length ({min_chunk_length}) so chunk merging is more likely to be valid"
    )

    boundaries = detect_all_boundaries(doc)

    # Add document start/end
    start_boundary = Boundary(0, PATTERNS[-1])  # word-level boundary
    end_boundary = Boundary(len(doc), PATTERNS[-1])
    boundaries = deque([start_boundary] + boundaries)

    result_chunks = []
    boundary_stats = defaultdict(int)
    start_pos = 0

    while start_pos < len(doc):
        # Handle overlap for chunks after the first
        actual_start_pos = start_pos
        if len(result_chunks) > 0 and chunk_max_overlap > 0:
            overlap_start = doc_tokens.tokens_forward_from_char(
                start_pos, -chunk_max_overlap
            )

            # Find boundaries in overlap range
            overlap_boundaries = []
            for boundary in boundaries:
                if overlap_start <= boundary.pos < start_pos:
                    overlap_boundaries.append(boundary)

            if overlap_boundaries:
                # Select best boundary in overlap range (prefer maximum overlap)
                best_overlap_boundary = min(
                    overlap_boundaries,
                    key=lambda b: boundary_cost(b, overlap_start, doc_tokens),
                )
                actual_start_pos = best_overlap_boundary.pos

        remaining_tokens = doc_tokens.char_span_to_tokens(actual_start_pos, len(doc))

        # Find preferred position based on token count
        preferred_pos = doc_tokens.tokens_forward_from_char(
            actual_start_pos, preferred_length_tokens
        )
        if preferred_pos >= len(doc):
            result_chunks.append(
                Chunk(
                    actual_start_pos,
                    doc[actual_start_pos:],
                    0,
                    remaining_tokens,
                    BPat("last_chunk_by_chars", [r"$(?!.)"], None, None),
                    None,  # placeholder: section_heading
                )
            )
            boundary_stats["last_chunk_by_chars"] += 1
            break

        # Find all valid boundaries for this chunk
        short_boundaries = []
        valid_boundaries = []
        while boundaries and boundaries[0].pos <= actual_start_pos:
            boundaries.popleft()  # discard any that are living in the past

        for boundary in boundaries:
            chunk_tokens = doc_tokens.char_span_to_tokens(
                actual_start_pos, boundary.pos
            )
            if chunk_tokens > max_chunk_length:
                break
            if chunk_tokens < min_chunk_length:
                short_boundaries.append(boundary)
            else:
                valid_boundaries.append(boundary)

        # If no valid boundaries, find any boundary that satisfies min_chunk_length
        if not valid_boundaries:
            valid_boundaries = short_boundaries  # should basically never happen though
            logger.warning("had to use short_boundaries!")
        if not valid_boundaries:
            logger.warning(
                "no valid boundaries or short boundaries - only happens with no spaces in text, cutting at preferred position"
            )
            result_chunks.append(
                Chunk(
                    actual_start_pos,
                    doc[actual_start_pos:preferred_pos],
                    len(doc) - preferred_pos,
                    doc_tokens.char_span_to_tokens(actual_start_pos, preferred_pos),
                    BPat("chars", [r"."], None, None),
                    None,  # placeholder: section_heading
                )
            )
            boundary_stats["emergency_fallback"] += 1
            start_pos = preferred_pos
            continue

        # Score and select best boundary
        best_boundary = min(
            valid_boundaries, key=lambda b: boundary_cost(b, preferred_pos, doc_tokens)
        )

        result_chunks.append(
            Chunk(
                actual_start_pos,
                doc[actual_start_pos : best_boundary.pos],
                len(doc) - best_boundary.pos,
                doc_tokens.char_span_to_tokens(actual_start_pos, best_boundary.pos),
                best_boundary.pattern,
                None,  # placeholder: section_heading
            )
        )

        # Record boundary type used
        boundary_stats[best_boundary.pattern.name] += 1

        start_pos = best_boundary.pos

    # Handle undersized final chunk by merging with previous (if merge wouldn't exceed max)
    if len(result_chunks) >= 2:
        final_tokens = doc_tokens.char_span_to_tokens(
            result_chunks[-1].start_pos,
            result_chunks[-1].start_pos + len(result_chunks[-1].value),
        )
        merged_tokens = doc_tokens.char_span_to_tokens(
            result_chunks[-2].start_pos, len(doc)
        )
        if final_tokens < min_chunk_length and merged_tokens <= max_chunk_length:
            result_chunks[-2:] = [
                Chunk(
                    result_chunks[-2].start_pos,
                    doc[result_chunks[-2].start_pos :],
                    0,
                    merged_tokens,
                    BPat(
                        "merge_last",
                        [result_chunks[-2].pat, result_chunks[-1].pat],
                        None,
                        None,
                    ),
                    None,  # placeholder: section_heading
                )
            ]
            boundary_stats["merged_last"] += 1

    # Fill in section headings for all chunks
    chunks_with_headings = [
        Chunk(
            chunk.start_pos,
            chunk.value,
            chunk.remain_chars,
            chunk.tokencount,
            chunk.pat,
            heading_at_position(headings, chunk.start_pos),
        )
        for chunk in result_chunks
    ]

    return chunks_with_headings, dict(boundary_stats)


def split_text(text: str) -> list[Chunk]:
    """Split text into chunks with section heading metadata."""
    chunks_list, _ = chunks(
        doc=text,
        min_chunk_length=MIN_CHUNK_LENGTH,
        max_chunk_length=MAX_CHUNK_LENGTH,
        preferred_length_tokens=PREFERRED_CHUNK_LENGTH,
        chunk_max_overlap=CHUNK_MAX_OVERLAP,
    )
    return chunks_list
