from unittest.mock import patch
from dateutil.parser import parse

from align_data.sources.stampy import Stampy


def test_validate_coda_token():
    dataset = Stampy(name="bla")
    with patch("align_data.stampy.stampy.CODA_TOKEN", None):
        with patch("sys.exit") as mock:
            dataset.setup()
            assert mock.called_once_with(1)


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
