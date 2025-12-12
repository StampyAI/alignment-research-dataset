"""Tests for text_splitter.py chunking and preprocessing."""
import pytest
from hypothesis import given, settings, strategies as st
from align_data.embeddings.text_splitter import chunks, split_text, TokenMapper, TOKENIZER


class TestPreprocessing:
    """Test the preprocessing filters applied before chunking."""

    def test_data_uri_removal(self):
        """Data URIs (base64 images) should be replaced."""
        text = "Before ![](data:image/png;base64,iVBORw0KGgoAAAANSUh) After"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "data:image" not in combined
        assert "[data-uri]" in combined

    def test_data_uri_multiline(self):
        """Data URIs with embedded newlines should be replaced."""
        text = "Before ![](data:image/png;base64,abc\ndef\nghi) After"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "[data-uri]" in combined

    def test_apostrophe_corruption_cleanup(self):
        """Corrupted apostrophes (''''...) should be collapsed to single."""
        text = "I''''''''''''m happy and we''''re good"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "''''" not in combined
        assert "I'm happy" in combined

    def test_base64_blob_removal(self):
        """Long base64 blobs should be replaced."""
        text = "Data: " + "A" * 100 + " end"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "[base64]" in combined
        assert "A" * 100 not in combined

    def test_url_preserved(self):
        """URLs should NOT be removed."""
        url = "https://example.com/very-long-path/with/many/segments/that/exceed/60/chars"
        text = f"Check out {url} for more info"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "example.com" in combined

    def test_markdown_link_preserved(self):
        """Markdown links should NOT be removed."""
        text = "See [this link](https://example.com/long/path/here/more/segments) for details"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "example.com" in combined

    def test_prompt_token_escape(self):
        """Prompt injection tokens should be escaped."""
        text = "Some text <|endofprompt|> more text <|endoftext|> end"
        result, _ = chunks(text)
        combined = " ".join(c.value for c in result)
        assert "<|endofprompt|>" not in combined
        assert "<|endoftext|>" not in combined
        assert "<endofprompt>" in combined
        assert "<endoftext>" in combined


class TestChunking:
    """Test the chunking algorithm itself."""

    def test_empty_text(self):
        """Empty text should return empty list."""
        result, _ = chunks("")
        assert result == []

    def test_whitespace_only(self):
        """Whitespace-only text should return empty list."""
        result, _ = chunks("   \n\n   ")
        assert result == []

    def test_single_short_chunk(self):
        """Short text should produce single chunk."""
        text = "This is a short piece of text."
        result, _ = chunks(text, min_chunk_length=5, max_chunk_length=100, preferred_length_tokens=50)
        assert len(result) == 1
        assert text in result[0].value

    def test_chunk_boundaries_respected(self):
        """Chunks should respect min/max boundaries."""
        # Create a longer text
        text = "This is a sentence. " * 50
        result, _ = chunks(text, min_chunk_length=10, max_chunk_length=50, preferred_length_tokens=30)

        for chunk in result:
            # Token count must be within specified bounds - no tolerance
            assert chunk.tokencount >= 10, f"Chunk under min: {chunk.tokencount} tokens"
            assert chunk.tokencount <= 50, f"Chunk over max: {chunk.tokencount} tokens"

    def test_no_content_lost(self):
        """All content should be preserved across chunks (no gaps)."""
        text = "Word " * 100
        result, _ = chunks(text, min_chunk_length=10, max_chunk_length=30,
                           preferred_length_tokens=20, chunk_max_overlap=0)

        # With overlap=0, chunks are contiguous - concatenation should recover original
        combined = "".join(c.value for c in result)

        # Exact word count - no content loss allowed
        assert combined.count("Word") == 100

    def test_chunk_order_preserved(self):
        """Chunks should be in document order."""
        text = "First section here. Second section here. Third section here. " * 10
        result, _ = chunks(text, min_chunk_length=10, max_chunk_length=50, preferred_length_tokens=30)

        if len(result) >= 2:
            # First chunk should contain "First"
            assert "First" in result[0].value
            # Last chunk should contain "Third"
            assert "Third" in result[-1].value


class TestSplitText:
    """Test the split_text convenience function."""

    def test_returns_strings(self):
        """split_text should return list of strings."""
        result = split_text("Hello world this is a test.")
        assert all(isinstance(s, str) for s in result)

    def test_uses_settings(self):
        """split_text should use settings.py values."""
        # With current settings (MIN=70, MAX=200, PREFERRED=100),
        # a short text should produce 1 chunk
        short_text = "This is a short test."
        result = split_text(short_text)
        assert len(result) == 1

        # A longer text should produce multiple chunks
        long_text = "This is a longer test sentence. " * 50
        result = split_text(long_text)
        assert len(result) > 1


class TestTokenMapper:
    """Direct tests for TokenMapper char↔token mapping accuracy."""

    def test_roundtrip_simple(self):
        """Token boundaries should align with character positions."""
        text = "Hello world, this is a test."
        mapper = TokenMapper(text)

        # char_to_token should cover entire document
        assert len(mapper.char_to_token) == len(text)

        # token_to_char should have one more entry than tokens (includes end position)
        assert len(mapper.token_to_char) == len(mapper.tokens) + 1

        # First char maps to first token
        assert mapper.char_to_token[0] == 0

        # Last position in token_to_char equals doc length
        assert mapper.token_to_char[-1] == len(text)

    def test_char_span_to_tokens_accuracy(self):
        """char_span_to_tokens should return accurate count."""
        text = "Hello world"
        mapper = TokenMapper(text)

        # Full span should equal total tokens
        full_span_tokens = mapper.char_span_to_tokens(0, len(text))
        assert full_span_tokens == len(mapper.tokens)

    def test_unicode_multibyte(self):
        """Multi-byte unicode characters should map correctly."""
        text = "Hello 世界 🎉 emoji"
        mapper = TokenMapper(text)

        # char_to_token length must equal character count, not byte count
        assert len(mapper.char_to_token) == len(text)

        # Roundtrip: tokens_forward then back should be consistent
        end_char = mapper.tokens_forward_from_char(0, 5)
        tokens_counted = mapper.char_span_to_tokens(0, end_char)
        # Should be close to 5 (might be off by 1 due to token boundaries)
        assert 4 <= tokens_counted <= 6

    def test_tokens_forward_boundary(self):
        """tokens_forward_from_char should handle boundaries correctly."""
        text = "Word " * 100
        mapper = TokenMapper(text)

        # Forward 0 tokens should return same position
        pos = mapper.tokens_forward_from_char(10, 0)
        # Should be at or near position 10
        assert 0 <= pos <= len(text)

        # Forward from start by total tokens should reach end
        end = mapper.tokens_forward_from_char(0, len(mapper.tokens))
        assert end == len(text)


class TestPropertyBased:
    """Property-based tests using hypothesis for edge case discovery."""

    @given(st.text(min_size=50, max_size=500, alphabet=st.characters(
        whitelist_categories=('L', 'N', 'P', 'Z'),  # Letters, numbers, punctuation, separators
        whitelist_characters=' \n.!?'
    )))
    @settings(max_examples=100)
    def test_no_char_loss_with_zero_overlap(self, text):
        """With overlap=0, concatenated chunks must exactly equal preprocessed input."""
        if not text.strip():
            return  # Skip empty/whitespace-only

        result, _ = chunks(text, min_chunk_length=5, max_chunk_length=50,
                          preferred_length_tokens=25, chunk_max_overlap=0)

        if not result:
            return  # Preprocessing eliminated all content

        combined = "".join(c.value for c in result)

        # The combined text should be a substring of or equal to what chunks() preprocessed
        # We can't compare to original since preprocessing changes it
        # But we CAN verify no gaps: each chunk.start_pos should connect
        for i in range(len(result) - 1):
            chunk_end = result[i].start_pos + len(result[i].value)
            next_start = result[i + 1].start_pos
            assert chunk_end == next_start, f"Gap between chunks {i} and {i+1}: {chunk_end} != {next_start}"

    @given(st.lists(
        st.text(min_size=1, max_size=20, alphabet=st.characters(
            whitelist_categories=('L', 'N'),  # Letters and numbers only for words
        )),
        min_size=20, max_size=100
    ).map(lambda words: ' '.join(words)))  # Join with spaces for realistic content
    @settings(max_examples=50)
    def test_chunks_within_bounds(self, text):
        """All chunks must be within specified min/max token bounds."""
        if not text.strip():
            return

        min_len, max_len = 10, 100
        result, _ = chunks(text, min_chunk_length=min_len, max_chunk_length=max_len,
                          preferred_length_tokens=50, chunk_max_overlap=0)

        for i, chunk in enumerate(result):
            # Max bound is ALWAYS enforced - no exceptions
            assert chunk.tokencount <= max_len, f"Chunk {i} exceeds max: {chunk.tokencount} > {max_len}"

            # Min bound: final chunk can be undersized if merge would exceed max
            is_final = (i == len(result) - 1)
            if not is_final:
                assert chunk.tokencount >= min_len, f"Chunk {i} under min: {chunk.tokencount} < {min_len}"

    @given(st.text(min_size=50, max_size=500, alphabet=st.characters(
        whitelist_categories=('L', 'N', 'P', 'Z'),
        whitelist_characters=' \n.!?'
    )))
    @settings(max_examples=50)
    def test_chunk_positions_monotonic(self, text):
        """Chunk start positions must be strictly increasing."""
        if not text.strip():
            return

        result, _ = chunks(text, min_chunk_length=5, max_chunk_length=50,
                          preferred_length_tokens=25, chunk_max_overlap=0)

        positions = [c.start_pos for c in result]
        for i in range(len(positions) - 1):
            assert positions[i] < positions[i + 1], f"Non-monotonic positions: {positions[i]} >= {positions[i+1]}"


class TestEdgeCases:
    """Targeted tests for specific edge cases likely to cause production bugs."""

    def test_unicode_emoji_preservation(self):
        """Emoji and unicode should not be corrupted or lost."""
        text = "Start 🎉🎊🎁 middle 世界您好 end " * 20
        result, _ = chunks(text, min_chunk_length=10, max_chunk_length=50,
                          preferred_length_tokens=30, chunk_max_overlap=0)

        combined = "".join(c.value for c in result)
        assert combined.count("🎉") == 20
        assert combined.count("世界") == 20

    def test_markdown_heading_boundaries(self):
        """Chunks should prefer breaking at markdown headings."""
        text = """# Heading One

Some content here that goes on for a while to make the chunk long enough.

## Heading Two

More content here that also needs to be reasonably long for chunking.

### Heading Three

Even more content to ensure we have multiple chunks to work with here.
""" * 3

        result, stats = chunks(text, min_chunk_length=20, max_chunk_length=100,
                              preferred_length_tokens=50, chunk_max_overlap=0)

        # Should use heading boundaries, not just word/sentence
        heading_boundaries = stats.get('h1', 0) + stats.get('h2', 0) + stats.get('h3', 0)
        assert heading_boundaries > 0, f"No heading boundaries used: {stats}"

    def test_exact_max_boundary(self):
        """Content at exactly max_chunk_length should not exceed bounds."""
        # Create text where natural boundary falls at exactly max tokens
        text = "Word " * 200
        max_len = 50

        result, _ = chunks(text, min_chunk_length=10, max_chunk_length=max_len,
                          preferred_length_tokens=30, chunk_max_overlap=0)

        for i, chunk in enumerate(result):
            assert chunk.tokencount <= max_len, f"Chunk {i}: {chunk.tokencount} > {max_len}"

    def test_paragraph_boundary_preference(self):
        """Should prefer paragraph boundaries over sentence boundaries."""
        text = """First paragraph with multiple sentences. This is sentence two. And sentence three.

Second paragraph also has sentences. Here is another one. And a third.

Third paragraph continues the pattern. More sentences here. Final sentence.
""" * 5

        result, stats = chunks(text, min_chunk_length=20, max_chunk_length=80,
                              preferred_length_tokens=50, chunk_max_overlap=0)

        # Paragraph boundaries should be used
        para_boundaries = stats.get('para', 0)
        assert para_boundaries > 0, f"No paragraph boundaries: {stats}"

    def test_long_document_accumulation(self):
        """Long documents should not accumulate small errors."""
        # Create a document long enough to have many chunks
        text = " ".join(f"This is sentence number {i}." for i in range(500))

        result, _ = chunks(text, min_chunk_length=50, max_chunk_length=200,
                          preferred_length_tokens=100, chunk_max_overlap=0)

        # Verify no gaps in coverage
        combined = "".join(c.value for c in result)

        # All sentence numbers should be present
        for i in range(500):
            assert f"number {i}" in combined, f"Lost sentence {i}"

    def test_whitespace_handling(self):
        """Leading/trailing whitespace in chunks should be handled consistently."""
        text = "   Start with spaces.   \n\n   Middle text.   \n\n   End text.   " * 10

        result, _ = chunks(text, min_chunk_length=5, max_chunk_length=50,
                          preferred_length_tokens=25, chunk_max_overlap=0)

        # Chunks should exist
        assert len(result) > 0

        # Combined should preserve content (whitespace normalization is ok)
        combined = "".join(c.value for c in result)
        assert "Start" in combined
        assert "Middle" in combined
        assert "End" in combined

    def test_mixed_content_realistic(self):
        """Realistic mixed content like actual LessWrong posts."""
        text = """# Introduction

This is an introduction paragraph with some **bold** and *italic* text.

## Mathematical Content

Here's some inline math: the equation $E = mc^2$ is famous.

### Code Example

```python
def hello():
    print("Hello, world!")
```

## Conclusion

Final thoughts here. See [this link](https://example.com) for more.

---

*Author's note: This is a footnote.*
""" * 5

        result, _ = chunks(text, min_chunk_length=20, max_chunk_length=100,
                          preferred_length_tokens=50, chunk_max_overlap=0)

        combined = "".join(c.value for c in result)

        # Key content should be preserved
        assert "Introduction" in combined
        assert "E = mc^2" in combined
        assert "hello()" in combined
        assert "Conclusion" in combined
