import os
from unittest.mock import MagicMock, patch

import pytest

# Mock keys so importing the module (and its deps) doesn't require real creds.
os.environ.setdefault("OPENAI_API_KEY", "sk-mock-test-key")

from sqlalchemy.exc import OperationalError

from align_data.embeddings.pinecone.update_pinecone import _read_with_retry


def _op_error():
    # Shape of the real failure seen 2026-05-24 in the daily cron.
    return OperationalError(
        "SELECT articles.id ...",
        {},
        Exception("2013 (HY000): Lost connection to MySQL server during query"),
    )


@patch("align_data.embeddings.pinecone.update_pinecone.time.sleep")
def test_read_with_retry_recovers_from_dropped_connection(mock_sleep):
    """A mid-query connection drop on a READ retries and then succeeds.

    The save path already retried; the initial ID fetch (update_pinecone.py:72)
    and per-batch load did not, which is what actually killed the 05-24 run.
    """
    session = MagicMock()
    attempts = {"n": 0}

    def read():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _op_error()
        return ["id1", "id2"]

    result = _read_with_retry(session, read, what="ID fetch")

    assert result == ["id1", "id2"]
    assert attempts["n"] == 3
    # rollback clears the failed transaction so pool_pre_ping can hand back a live conn.
    assert session.rollback.call_count == 2


@patch("align_data.embeddings.pinecone.update_pinecone.time.sleep")
def test_read_with_retry_raises_loudly_when_exhausted(mock_sleep):
    """If the DB never comes back, we raise (can't index what we can't read) -- never
    swallow it silently, so the job fails loudly instead of indexing a partial set."""
    session = MagicMock()

    def read():
        raise _op_error()

    with pytest.raises(OperationalError):
        _read_with_retry(session, read, what="ID fetch", max_retries=3)


@patch("align_data.embeddings.pinecone.update_pinecone.time.sleep")
def test_read_with_retry_does_not_retry_other_errors(mock_sleep):
    """Non-connection errors (e.g. a bug) are not connection blips -- don't mask them."""
    session = MagicMock()

    def read():
        raise ValueError("programming error")

    with pytest.raises(ValueError):
        _read_with_retry(session, read, what="ID fetch")
