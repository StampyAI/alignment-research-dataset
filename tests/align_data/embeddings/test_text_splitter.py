"""Tests for text_splitter.py chunking and preprocessing."""
import pytest
from align_data.embeddings.text_splitter import chunks, split_text


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
            # Token count should be within bounds (with some tolerance for edge cases)
            assert chunk.tokencount >= 5  # Allow some under for final chunk
            assert chunk.tokencount <= 60  # Allow some over for boundary cases

    def test_no_content_lost(self):
        """All content should be preserved across chunks (no gaps)."""
        text = "Word " * 100
        result, _ = chunks(text, min_chunk_length=10, max_chunk_length=30, preferred_length_tokens=20)

        # Concatenate all chunks (accounting for overlap)
        # With overlap=0 (new default), chunks should be contiguous
        combined = ""
        for chunk in result:
            combined += chunk.value

        # All words should be present
        assert combined.count("Word") >= 90  # Allow for some preprocessing

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
