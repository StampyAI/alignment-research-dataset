import unittest
from unittest.mock import patch, MagicMock
import os
from typing import List, Tuple, Union

# Mock the OpenAI API key for tests
os.environ["OPENAI_API_KEY"] = "sk-mock-test-key"

from align_data.embeddings.pinecone.pinecone_db_handler import (
    PineconeDB,
    initialize_pinecone,
    with_retry,
    strip_block,
)


class TestPineconeInitialization:
    """Tests for Pinecone initialization."""

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.Pinecone")
    def test_initialize_pinecone(self, mock_pinecone):
        """Test initialization with the Pinecone API."""
        mock_client = MagicMock()
        mock_pinecone.return_value = mock_client

        client = initialize_pinecone()

        # Check that the API was called correctly
        mock_pinecone.assert_called_once()
        assert client == mock_client

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.Pinecone")
    def test_initialize_pinecone_with_error(self, mock_pinecone):
        """Test initialization with error handling."""
        mock_pinecone.side_effect = Exception("API Error")

        try:
            initialize_pinecone()
            assert False, "Should have raised an exception"
        except Exception as e:
            assert "API Error" in str(e)


class TestWithRetry:
    """Tests for the retry decorator."""

    def test_with_retry_success(self):
        """Test successful execution with retry decorator."""

        @with_retry(n=3)
        def test_func():
            return "success"

        result = test_func()
        assert result == "success"

    def test_with_retry_failure(self):
        """Test retry decorator with eventual failure."""
        call_count = 0

        @with_retry(n=3, exceptions=(ValueError,))
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Test error")

        try:
            test_func()
            assert False, "Should have raised TimeoutError"
        except TimeoutError:
            assert call_count == 3


class TestPineconeDB:
    """Tests for the PineconeDB class."""

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_init(self, mock_initialize):
        """Test PineconeDB initialization."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Create a test index
        mock_index = MagicMock()
        mock_client.Index.return_value = mock_index

        # Initialize PineconeDB
        db = PineconeDB(
            index_name="test-index",
            values_dims=1536,
            metric="cosine",
            create_index=False,
            log_index_stats=False,
        )

        # Verify client initialization
        mock_initialize.assert_called_once()
        assert db.pinecone == mock_client
        assert db.index == mock_index
        mock_client.Index.assert_called_once_with("test-index")

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_init_with_stats(self, mock_initialize):
        """Test PineconeDB initialization with stats logging."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Create a test index
        mock_index = MagicMock()
        mock_index.describe_index_stats.return_value = {"total_vector_count": 100}
        mock_client.Index.return_value = mock_index

        # Initialize PineconeDB with stats
        db = PineconeDB(
            index_name="test-index",
            log_index_stats=True,
        )

        # Verify stats were requested
        mock_index.describe_index_stats.assert_called_once()

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    @patch("align_data.embeddings.pinecone.pinecone_db_handler.ServerlessSpec")
    def test_create_index(self, mock_serverless_spec, mock_initialize):
        """Test create_index method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock ServerlessSpec
        mock_spec = MagicMock()
        mock_serverless_spec.return_value = mock_spec

        # Mock index listing
        mock_client.list_indexes.return_value = []

        # Initialize PineconeDB
        db = PineconeDB(
            index_name="test-index",
            values_dims=1536,
            metric="cosine",
            create_index=True,
        )

        # Verify create_index was called
        mock_client.create_index.assert_called_once()
        call_args = mock_client.create_index.call_args[1]
        assert call_args["name"] == "test-index"
        assert call_args["dimension"] == 1536
        assert call_args["metric"] == "cosine"
        assert call_args["spec"] == mock_spec

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_delete_index(self, mock_initialize):
        """Test delete_index method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index listing
        mock_client.list_indexes.return_value = ["test-index"]

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test delete_index
        db.delete_index()

        # Verify delete_index was called
        mock_client.delete_index.assert_called_once_with("test-index")

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_upsert_entry(self, mock_initialize):
        """Test upsert_entry method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index
        mock_index = MagicMock()
        mock_client.Index.return_value = mock_index

        # Mock PineconeEntry
        mock_entry = MagicMock()
        mock_vectors = [{"id": "test-id", "values": [0.1, 0.2, 0.3]}]
        mock_entry.create_pinecone_vectors.return_value = mock_vectors

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test upsert_entry
        db.upsert_entry(mock_entry)

        # Verify upsert was called
        mock_index.upsert.assert_called_once()
        call_args = mock_index.upsert.call_args[1]
        assert call_args["vectors"] == mock_vectors

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_query_vector(self, mock_initialize):
        """Test query_vector method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index
        mock_index = MagicMock()
        mock_client.Index.return_value = mock_index

        # Mock query response
        mock_response = {
            "matches": [
                {
                    "id": "doc1",
                    "score": 0.9,
                    "metadata": {
                        "title": "Test Document",
                        "text": "Test content",
                        "source": "test_source",
                    },
                }
            ]
        }
        mock_index.query.return_value = mock_response

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test query_vector
        results = db.query_vector([0.1, 0.2, 0.3], top_k=5)

        # Verify query was called
        mock_index.query.assert_called_once()
        call_args = mock_index.query.call_args[1]
        assert call_args["vector"] == [0.1, 0.2, 0.3]
        assert call_args["top_k"] == 5

        # Verify results
        assert len(results) == 1
        assert results[0]["id"] == "doc1"
        assert results[0]["score"] == 0.9
        assert results[0]["metadata"]["title"] == "Test Document"

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.get_embedding")
    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_query_text(self, mock_initialize, mock_get_embedding):
        """Test query_text method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index
        mock_index = MagicMock()
        mock_client.Index.return_value = mock_index

        # Mock embedding response
        mock_embedding = MagicMock()
        mock_embedding.vector = [0.1, 0.2, 0.3]
        mock_get_embedding.return_value = ([mock_embedding], None)

        # Mock query response
        mock_response = {
            "matches": [
                {
                    "id": "doc1",
                    "score": 0.9,
                    "metadata": {"title": "Test Document"},
                }
            ]
        }
        mock_index.query.return_value = mock_response

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test query_text
        results = db.query_text("test query", top_k=5)

        # Verify get_embedding was called
        mock_get_embedding.assert_called_once_with("test query")

        # Verify query was called with the embedding
        mock_index.query.assert_called_once()
        call_args = mock_index.query.call_args[1]
        assert call_args["vector"] == [0.1, 0.2, 0.3]

        # Verify results
        assert len(results) == 1
        assert results[0]["id"] == "doc1"

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_get_embeddings_by_ids(self, mock_initialize):
        """Test get_embeddings_by_ids method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index
        mock_index = MagicMock()
        mock_client.Index.return_value = mock_index

        # Mock fetch response
        mock_response = {
            "vectors": {
                "id1": {"values": [0.1, 0.2, 0.3]},
                "id2": {"values": [0.4, 0.5, 0.6]},
            }
        }
        mock_index.fetch.return_value = mock_response

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test get_embeddings_by_ids
        results = db.get_embeddings_by_ids(["id1", "id2", "id3"])

        # Verify fetch was called
        mock_index.fetch.assert_called_once()
        call_args = mock_index.fetch.call_args[1]
        assert call_args["ids"] == ["id1", "id2", "id3"]

        # Verify results
        assert len(results) == 3
        assert results[0] == ("id1", [0.1, 0.2, 0.3])
        assert results[1] == ("id2", [0.4, 0.5, 0.6])
        assert results[2] == ("id3", None)  # Non-existent ID returns None

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_delete_entries(self, mock_initialize):
        """Test delete_entries method."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index
        mock_index = MagicMock()
        mock_index.list.return_value = ["item1", "item2"]
        mock_client.Index.return_value = mock_index

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test delete_entries
        db.delete_entries(["id1", "id2"])

        # Verify list was called for each ID
        assert mock_index.list.call_count == 2

        # Verify delete was called
        mock_index.delete.assert_called_once()


class TestPineconeCompatibility:
    """Tests for API compatibility layers."""

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_upsert_with_error_handling(self, mock_initialize):
        """Test upsert with error handling for API differences."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index that throws TypeError on first upsert attempt
        mock_index = MagicMock()
        mock_index.upsert.side_effect = [
            TypeError("API incompatibility"),
            None,  # Second call succeeds
        ]
        mock_client.Index.return_value = mock_index

        # Mock vectors
        mock_vectors = [{"id": "id1", "values": [0.1, 0.2, 0.3]}]

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test _upsert with error handling
        db._upsert(mock_vectors)

        # Verify upsert was called twice (first fails, second succeeds)
        assert mock_index.upsert.call_count == 2

    @patch("align_data.embeddings.pinecone.pinecone_db_handler.initialize_pinecone")
    def test_query_with_error_handling(self, mock_initialize):
        """Test query with error handling for API differences."""
        mock_client = MagicMock()
        mock_initialize.return_value = mock_client

        # Mock index that throws TypeError on first query attempt
        mock_index = MagicMock()
        mock_response = {"matches": []}
        mock_index.query.side_effect = [
            TypeError("API incompatibility"),
            mock_response,  # Second call succeeds
        ]
        mock_client.Index.return_value = mock_index

        # Initialize PineconeDB
        db = PineconeDB(index_name="test-index")

        # Test query_vector with error handling
        results = db.query_vector([0.1, 0.2, 0.3])

        # Verify query was called twice (first fails, second succeeds)
        assert mock_index.query.call_count == 2
        assert len(results) == 0


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_strip_block(self):
        """Test strip_block utility function."""
        test_text = "first line\nsecond line\nthird line"
        result = strip_block(test_text)
        assert result == "second line\nthird line"

    def test_strip_block_empty(self):
        """Test strip_block with empty input."""
        result = strip_block("")
        assert result == ""

    def test_strip_block_single_line(self):
        """Test strip_block with single line."""
        result = strip_block("single line")
        assert result == ""


if __name__ == "__main__":
    unittest.main()
