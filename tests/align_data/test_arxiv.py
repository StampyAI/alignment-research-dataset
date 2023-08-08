from datetime import datetime
from unittest.mock import patch, Mock
import pytest
from align_data.sources.arxiv_papers.arxiv_papers import ArxivPapers


@pytest.mark.parametrize(
    "url, expected",
    (
        ("https://arxiv.org/abs/2001.11038", "2001.11038"),
        ("https://arxiv.org/abs/2001.11038/", "2001.11038"),
        ("https://bla.bla/2001.11038/", None),
    ),
)
def test_get_id(url, expected):
    dataset = ArxivPapers(name="asd", spreadsheet_id="ad", sheet_id="da")
    assert dataset.get_id(Mock(url="https://arxiv.org/abs/2001.11038")) == "2001.11038"


def test_process_entry():
    dataset = ArxivPapers(name="asd", spreadsheet_id="ad", sheet_id="da")
    item = Mock(
        title="this is the title",
        url="https://arxiv.org/abs/2001.11038",
        authors="",
        date_published="2020-01-29",
    )
    contents = {
        "text": "this is the text",
        "date_published": "December 12, 2021",
        "authors": ["mr blobby"],
        "data_source": "html",
    }
    metadata = Mock(
        summary="abstract bla bla",
        comment="no comment",
        categories="wut",
        updated="2023-01-01",
        authors=[],
        doi="123",
        journal_ref="sdf",
        primary_category="cat",
    )
    arxiv = Mock()
    arxiv.Search.return_value.results.return_value = iter([metadata])

    with patch(
        "align_data.arxiv_papers.arxiv_papers.parse_vanity", return_value=contents
    ):
        with patch("align_data.arxiv_papers.arxiv_papers.arxiv", arxiv):
            assert dataset.process_entry(item).to_dict() == {
                "author_comment": "no comment",
                "authors": ["mr blobby"],
                "categories": "wut",
                "data_last_modified": "2023-01-01",
                "date_published": "2020-01-29T00:00:00Z",
                "doi": "123",
                "id": None,
                "journal_ref": "sdf",
                "primary_category": "cat",
                "source": "asd",
                "source_type": "html",
                "summaries": ["abstract bla bla"],
                "text": "this is the text",
                "title": "this is the title",
                "url": "https://arxiv.org/abs/2001.11038",
            }
