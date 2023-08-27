from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from align_data.db.models import Article
from align_data.sources.articles.datasets import (
    ArxivPapers,
    EbookArticles,
    DocArticles,
    HTMLArticles,
    MarkdownArticles,
    PDFArticles,
    SpreadsheetDataset,
    SpecialDocs,
    XMLArticles,
)


@pytest.fixture
def articles():
    source_type = "something"
    articles = [
        {
            "source_url": f"http://example.com/source_url/{i}",
            "url": f"http://example.com/item/{i}",
            "title": f"article no {i}",
            "source_type": source_type,
            "date_published": f"2023/01/0{i + 1} 12:32:11",
            "authors": f"John Snow, mr Blobby",
            "summary": f"the summary of article {i}",
            "file_id": str(i),
        }
        for i in range(5)
    ]
    return pd.DataFrame(articles)


@pytest.fixture
def mock_arxiv():
    metadata = Mock(
        summary="abstract bla bla",
        comment="no comment",
        categories="wut",
        updated=datetime.fromisoformat("2023-01-01T00:00:00"),
        authors=[],
        doi="123",
        journal_ref="sdf",
        primary_category="cat",
    )
    metadata.get_short_id.return_value = "2001.11038"
    arxiv = Mock()
    arxiv.Search.return_value.results.return_value = iter([metadata])

    with patch("align_data.sources.arxiv_papers.arxiv", arxiv):
        yield


def test_spreadsheet_dataset_items_list(articles):
    dataset = SpreadsheetDataset(name="bla", spreadsheet_id="123", sheet_id="456")
    df = pd.concat(
        [articles, pd.DataFrame([{"title": None}, {"summary": "bla"}])],
        ignore_index=True,
    )
    with patch("pandas.read_csv", return_value=df):
        assert list(dataset.items_list) == list(pd.DataFrame(articles).itertuples())


def test_spreadsheet_dataset_get_item_key():
    dataset = SpreadsheetDataset(name="bla", spreadsheet_id="123", sheet_id="456")
    assert dataset.get_item_key(Mock(bla="ble", url="the key")) == "the key"


@pytest.mark.parametrize(
    "authors, expected",
    (
        ("", []),
        ("   \n \n  \t", []),
        ("John Snow", ["John Snow"]),
        ("John Snow, mr. Blobby", ["John Snow", "mr. Blobby"]),
    ),
)
def test_spreadsheet_dataset_extract_authors(authors, expected):
    dataset = SpreadsheetDataset(name="bla", spreadsheet_id="123", sheet_id="456")
    assert dataset.extract_authors(Mock(authors=authors)) == expected


def test_pdf_articles_get_text():
    dataset = PDFArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    item = Mock(file_id="23423", title="bla bla bla")

    def check_downloads(output, id):
        assert output == str(dataset.files_path / "bla bla bla.pdf")
        assert id == "23423"
        return output

    def read_pdf(filename):
        assert filename == dataset.files_path / "bla bla bla.pdf"
        return "pdf contents"

    with patch("align_data.sources.articles.datasets.download", check_downloads):
        with patch("align_data.sources.articles.datasets.read_pdf", read_pdf):
            assert dataset._get_text(item) == "pdf contents"


def test_pdf_articles_process_item(articles):
    dataset = PDFArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("pandas.read_csv", return_value=articles):
        item = list(dataset.items_list)[0]

    with patch("align_data.sources.articles.datasets.download"):
        with patch(
            "align_data.sources.articles.datasets.read_pdf",
            return_value='pdf contents <a href="asd.com">bla</a>',
        ):
            assert dataset.process_entry(item).to_dict() == {
                "authors": ["John Snow", "mr Blobby"],
                "date_published": "2023-01-01T12:32:11Z",
                "id": None,
                "source": "bla",
                "source_filetype": "pdf",
                "source_type": "something",
                "summaries": ["the summary of article 0"],
                "text": "pdf contents [bla](asd.com)",
                "title": "article no 0",
                "url": "http://example.com/item/0",
                "source_url": "http://example.com/source_url/0",
            }


def test_html_articles_get_text():
    def parser(url):
        assert url == "http://example.org/bla.bla"
        return {"text": "html contents"}

    with patch("align_data.sources.articles.datasets.HTML_PARSERS", {"example.org": parser}):
        assert (
            HTMLArticles._get_text(Mock(source_url="http://example.org/bla.bla")) == "html contents"
        )


def test_html_articles_get_text_no_parser():
    with patch("align_data.sources.articles.datasets.HTML_PARSERS", {}):
        assert HTMLArticles._get_text(Mock(source_url="http://example.org/bla.bla")) is None


def test_html_articles_process_entry(articles):
    dataset = HTMLArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("pandas.read_csv", return_value=articles):
        item = list(dataset.items_list)[0]

    parsers = {
        "example.com": lambda _: {
            "text": '   html contents with <a href="bla.com">proper elements</a> ble ble   '
        }
    }
    with patch("align_data.sources.articles.datasets.HTML_PARSERS", parsers):
        assert dataset.process_entry(item).to_dict() == {
            "authors": ["John Snow", "mr Blobby"],
            "date_published": "2023-01-01T12:32:11Z",
            "id": None,
            "source": "bla",
            "source_filetype": "html",
            "source_type": "something",
            "summaries": ["the summary of article 0"],
            "text": "html contents with [proper elements](bla.com) ble ble",
            "title": "article no 0",
            "url": "http://example.com/item/0",
            "source_url": "http://example.com/source_url/0",
        }


def test_ebook_articles_get_text():
    dataset = EbookArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    item = Mock(
        source_url="https://drive.google.com/file/d/123456/view?usp=drive_link",
        title="bla bla bla",
    )

    def check_downloads(output, id):
        assert output == str(dataset.files_path / "bla bla bla.epub")
        assert id == "123456"
        return output

    def read_ebook(filename, *args, **kwargs):
        return "ebook contents"

    with patch("align_data.sources.articles.datasets.download", check_downloads):
        with patch("align_data.sources.articles.datasets.convert_file", read_ebook):
            assert dataset._get_text(item) == "ebook contents"


def test_ebook_articles_process_entry(articles):
    dataset = EbookArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("pandas.read_csv", return_value=articles):
        item = list(dataset.items_list)[0]

    contents = '   html contents with <a href="bla.com">proper elements</a> ble ble   '
    with patch("align_data.sources.articles.datasets.download"):
        with patch("align_data.sources.articles.datasets.convert_file", return_value=contents):
            assert dataset.process_entry(item).to_dict() == {
                "authors": ["John Snow", "mr Blobby"],
                "date_published": "2023-01-01T12:32:11Z",
                "id": None,
                "source": "bla",
                "source_filetype": "epub",
                "source_type": "something",
                "summaries": ["the summary of article 0"],
                "text": "html contents with [proper elements](bla.com) ble ble",
                "title": "article no 0",
                "url": "http://example.com/item/0",
                "source_url": "http://example.com/source_url/0",
            }


def test_xml_articles_get_text():
    dataset = XMLArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch(
        "align_data.sources.articles.datasets.extract_gdrive_contents",
        return_value={"text": "bla bla"},
    ):
        assert dataset._get_text(Mock(source_url="bla.com")) == "bla bla"


def test_xml_articles_process_entry(articles):
    dataset = XMLArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("pandas.read_csv", return_value=articles):
        item = list(dataset.items_list)[0]

    with patch(
        "align_data.sources.articles.datasets.extract_gdrive_contents",
        return_value={"text": "bla bla"},
    ):
        assert dataset.process_entry(item).to_dict() == {
            "authors": ["John Snow", "mr Blobby"],
            "date_published": "2023-01-01T12:32:11Z",
            "id": None,
            "source": "bla",
            "source_filetype": "xml",
            "source_type": "something",
            "summaries": ["the summary of article 0"],
            "text": "bla bla",
            "title": "article no 0",
            "url": "http://example.com/item/0",
            "source_url": "http://example.com/source_url/0",
        }


def test_markdown_articles_get_text():
    dataset = MarkdownArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch(
        "align_data.sources.articles.datasets.fetch_markdown",
        return_value={"text": "bla bla"},
    ):
        assert dataset._get_text(Mock(source_url="bla.com/bla/123/bla")) == "bla bla"


def test_markdown_articles_process_entry(articles):
    dataset = MarkdownArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("pandas.read_csv", return_value=articles):
        item = list(dataset.items_list)[0]

    with patch(
        "align_data.sources.articles.datasets.fetch_markdown",
        return_value={"text": "bla bla"},
    ):
        assert dataset.process_entry(item).to_dict() == {
            "authors": ["John Snow", "mr Blobby"],
            "date_published": "2023-01-01T12:32:11Z",
            "id": None,
            "source": "bla",
            "source_filetype": "markdown",
            "source_type": "something",
            "summaries": ["the summary of article 0"],
            "text": "bla bla",
            "title": "article no 0",
            "url": "http://example.com/item/0",
            "source_url": "http://example.com/source_url/0",
        }


def test_doc_articles_get_text():
    dataset = DocArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("align_data.sources.articles.datasets.fetch_file"):
        with patch("align_data.sources.articles.datasets.convert_file", return_value="bla bla"):
            assert dataset._get_text(Mock(source_url="bla.com/bla/123/bla")) == "bla bla"


def test_doc_articles_process_entry(articles):
    dataset = DocArticles(name="bla", spreadsheet_id="123", sheet_id="456")
    with patch("pandas.read_csv", return_value=articles):
        item = list(dataset.items_list)[0]

    with patch("align_data.sources.articles.datasets.fetch_file"):
        with patch("align_data.sources.articles.datasets.convert_file", return_value="bla bla"):
            assert dataset.process_entry(item).to_dict() == {
                "authors": ["John Snow", "mr Blobby"],
                "date_published": "2023-01-01T12:32:11Z",
                "id": None,
                "source": "bla",
                "source_filetype": "docx",
                "source_type": "something",
                "summaries": ["the summary of article 0"],
                "text": "bla bla",
                "title": "article no 0",
                "url": "http://example.com/item/0",
                "source_url": "http://example.com/source_url/0",
            }


@patch("requests.get", return_value=Mock(content=""))
def test_arxiv_process_entry(_, mock_arxiv):
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
        "source_type": "html",
    }
    with patch("align_data.sources.arxiv_papers.parse_vanity", return_value=contents):
        assert dataset.process_entry(item).to_dict() == {
            "comment": "no comment",
            "authors": ["mr blobby"],
            "categories": "wut",
            "data_last_modified": "2023-01-01T00:00:00",
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


def test_arxiv_process_entry_retracted(mock_arxiv):
    dataset = ArxivPapers(name="asd", spreadsheet_id="ad", sheet_id="da")
    item = Mock(
        title="this is the title",
        url="https://arxiv.org/abs/2001.11038",
        authors="",
        date_published="2020-01-29",
    )
    response = """
    <div class="extra-services">
      <div class="full-text">
        <a name="other"></a>
        <span class="descriptor">Full-text links:</span>
        <h2>Download:</h2>
        <ul><li>Withdrawn</li></ul>
        <div class="abs-license"><div hidden="">No license for this version due to withdrawn</div></div>
      </div>
    </div>
    """

    with patch("requests.get", return_value=Mock(content=response)):
        article = dataset.process_entry(item)
        assert article.status == "Withdrawn"
        assert article.to_dict() == {
            "comment": "no comment",
            "authors": [],
            "categories": "wut",
            "data_last_modified": "2023-01-01T00:00:00",
            "date_published": "2020-01-29T00:00:00Z",
            "doi": "123",
            "id": None,
            "journal_ref": "sdf",
            "primary_category": "cat",
            "source": "asd",
            "source_type": None,
            "summaries": ["abstract bla bla"],
            "title": "this is the title",
            "url": "https://arxiv.org/abs/2001.11038",
            "text": None,
        }


def test_special_docs_process_entry():
    dataset = SpecialDocs(name="asd", spreadsheet_id="ad", sheet_id="da")
    item = Mock(
        title="this is the title",
        url="https://bla.bla.bla",
        authors="mr. blobby",
        date_published="2023-10-02T01:23:45",
        source_type=None,
        source_url="https://ble.ble.com",
    )
    contents = {
        "text": "this is the text",
        "date_published": "December 12, 2021",
        "authors": ["mr blobby"],
        "source_type": "html",
    }

    with patch("align_data.sources.articles.datasets.item_metadata", return_value=contents):
        assert dataset.process_entry(item).to_dict() == {
            "authors": ["mr. blobby"],
            "date_published": "2023-10-02T01:23:45Z",
            "id": None,
            "source": "html",
            "source_url": "https://ble.ble.com",
            "source_type": "html",
            "summaries": [],
            "text": "this is the text",
            "title": "this is the title",
            "url": "https://bla.bla.bla",
        }


@patch("requests.get", return_value=Mock(content=""))
def test_special_docs_process_entry_arxiv(_, mock_arxiv):
    dataset = SpecialDocs(name="asd", spreadsheet_id="ad", sheet_id="da")
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
        "source_type": "pdf",
    }

    with patch("align_data.sources.arxiv_papers.parse_vanity", return_value=contents):
        assert dataset.process_entry(item).to_dict() == {
            "comment": "no comment",
            "authors": ["mr blobby"],
            "categories": "wut",
            "data_last_modified": "2023-01-01T00:00:00",
            "date_published": "2020-01-29T00:00:00Z",
            "doi": "123",
            "id": None,
            "journal_ref": "sdf",
            "primary_category": "cat",
            "source": "arxiv",
            "source_type": "pdf",
            "summaries": ["abstract bla bla"],
            "text": "this is the text",
            "title": "this is the title",
            "url": "https://arxiv.org/abs/2001.11038",
        }


@pytest.mark.parametrize(
    "url, expected",
    (
        ("http://bla.bla", "http://bla.bla"),
        ("http://arxiv.org/abs/2001.11038", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/abs/2001.11038", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/abs/2001.11038/", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/pdf/2001.11038", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/pdf/2001.11038.pdf", "https://arxiv.org/abs/2001.11038"),
        ("https://arxiv.org/pdf/2001.11038v3.pdf", "https://arxiv.org/abs/2001.11038"),
        (
            "https://arxiv.org/abs/math/2001.11038",
            "https://arxiv.org/abs/math/2001.11038",
        ),
    ),
)
def test_special_docs_not_processed_true(url, expected):
    dataset = SpecialDocs(name="asd", spreadsheet_id="ad", sheet_id="da")
    dataset._outputted_items = [url, expected]
    assert not dataset.not_processed(Mock(url=url, source_url=None))
    assert not dataset.not_processed(Mock(url=None, source_url=url))


@pytest.mark.parametrize(
    "url",
    (
        "http://bla.bla" "http://arxiv.org/abs/2001.11038",
        "https://arxiv.org/abs/2001.11038",
        "https://arxiv.org/abs/2001.11038/",
        "https://arxiv.org/pdf/2001.11038",
    ),
)
def test_special_docs_not_processed_false(url):
    dataset = SpecialDocs(name="asd", spreadsheet_id="ad", sheet_id="da")
    dataset._outputted_items = []
    assert dataset.not_processed(Mock(url=url, source_url=None))
    assert dataset.not_processed(Mock(url=None, source_url=url))
