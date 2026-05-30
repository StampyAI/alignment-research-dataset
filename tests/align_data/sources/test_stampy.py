from unittest.mock import MagicMock, patch

import pytest
import requests
from dateutil.parser import parse

from align_data.sources.stampy import Stampy
from align_data.sources.stampy import stampy as stampy_mod


def _resp(status=200, payload=None):
    r = MagicMock()
    r.status_code = status
    r.text = "" if payload is None else str(payload)
    r.json.return_value = payload
    return r


@patch("align_data.sources.stampy.stampy.time.sleep")
@patch("align_data.sources.stampy.stampy.requests.get")
def test_coda_get_retries_transient_timeout(mock_get, mock_sleep):
    """A transient Coda read-timeout (the 2026-05-30 daily failure) is retried, not fatal."""
    mock_get.side_effect = [
        requests.exceptions.ReadTimeout("read timed out"),
        requests.exceptions.ReadTimeout("read timed out"),
        _resp(200, {"items": [{"id": "c1", "name": "Question"}]}),
    ]
    assert stampy_mod.get_columns() == {"c1": "Question"}
    assert mock_get.call_count == 3


@patch("align_data.sources.stampy.stampy.time.sleep")
@patch("align_data.sources.stampy.stampy.requests.get")
def test_coda_get_raises_after_persistent_timeout(mock_get, mock_sleep):
    """If Coda stays unreachable, the last timeout propagates -- the fetch aborts before
    any DB write (get_rows materializes the full list first), so no partial corruption."""
    mock_get.side_effect = requests.exceptions.ReadTimeout("read timed out")
    with pytest.raises(requests.exceptions.RequestException):
        stampy_mod.get_columns()
    assert mock_get.call_count == 3


@patch("align_data.sources.stampy.stampy.time.sleep")
@patch("align_data.sources.stampy.stampy.requests.get")
def test_coda_get_does_not_retry_http_error(mock_get, mock_sleep):
    """A 4xx won't self-resolve -- fail fast on the first try, don't burn retries."""
    mock_get.return_value = _resp(404, {"message": "not found"})
    with pytest.raises(Exception):
        stampy_mod.get_columns()
    assert mock_get.call_count == 1


def test_validate_coda_token():
    dataset = Stampy(name="bla")
    with patch("align_data.stampy.stampy.CODA_TOKEN", None):
        with patch("sys.exit") as mock:
            dataset.setup()
            mock.assert_called_once_with(1)


def test_get_item_key():
    dataset = Stampy(name="bla")
    assert dataset.get_item_key({"Question": "Why&NewLine;not&#32;just&#63;"}) == "Why\nnot just?"


def test_get_published_date():
    dataset = Stampy(name="bla")
    assert dataset._get_published_date({"Doc Last Edited": "2012/01/03 12:23:32"}) == parse(
        "2012-01-03T12:23:32Z"
    )


def test_get_published_date_missing():
    dataset = Stampy(name="bla")
    assert dataset._get_published_date({"Doc Last Edited": ""}) == None


def test_process_entry():
    dataset = Stampy(name="bla")
    entry = {
        "Question": "Why&NewLine;not&#32;just&#63;",
        "Rich Text": "bla bla bla",
        "UI ID": "1234",
        "Doc Last Edited": "2012-02-03",
    }
    assert dataset.process_entry(entry).to_dict() == {
        "authors": ["Stampy aisafety.info"],
        "date_published": "2012-02-03T00:00:00Z",
        "id": None,
        "source": "bla",
        "source_type": "markdown",
        "summaries": [],
        "text": "bla bla bla",
        "title": "Why not just?",
        "url": "https://aisafety.info?state=1234",
    }
