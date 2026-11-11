from unittest.mock import patch, Mock

import pytest
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse

from align_data import settings
from align_data.common.html_dataset import HTMLDataset, RSSDataset


@pytest.fixture
def html_dataset():
    dataset = HTMLDataset(
        name="bla", url="http://example.com", authors=["John Smith", "Your momma"]
    )

    return dataset


SAMPLE_CONTENTS = """
  <div>

    bla bla bla <a href="http://ble.com">a link</a> bla bla


  </div>
"""
SAMPLE_HTML = f"""
<article>
  <h1>
       This is the title
  </h1>
  {SAMPLE_CONTENTS}
</article>
"""


def test_html_dataset_extract_authors(html_dataset: HTMLDataset):
    assert html_dataset.extract_authors("dummy variable") == [
        "John Smith",
        "Your momma",
    ]


def test_html_dataset_get_title(html_dataset: HTMLDataset):
    item = f"""
    <article>
      <h1>   This is the title
      </h1>
      <a href="{html_dataset.url}/path/to/article">click to read more</a>
    </article>
    """
    soup = BeautifulSoup(item, "html.parser")
    assert html_dataset._get_title(soup) == "This is the title"


def test_html_dataset_get_title_missing(html_dataset):
    soup = BeautifulSoup("", "html.parser")
    assert html_dataset._get_title(soup) is None


def test_html_dataset_get_item_key(html_dataset):
    item = f"""
    <div>
      <h2>the title</h2>
      <a href="{html_dataset.url}/path/to/article">click to read more</a>
    </div>
    """
    soup = BeautifulSoup(item, "html.parser")
    assert html_dataset.get_item_key(soup) == "http://example.com/path/to/article"


def test_html_dataset_items_list(html_dataset):
    text = """
    <div>
      <article>article 1</article>
      <article>article 2</article>
      <article>article 3</article>
      <article>article 4</article>
      <article>article 5</article>
    </div>
    """
    with patch("requests.get", return_value=Mock(content=text)):
        assert [i.text for i in html_dataset.items_list] == [
            "article 1",
            "article 2",
            "article 3",
            "article 4",
            "article 5",
        ]


def test_html_datasetfetch_contents(html_dataset):
    with patch("requests.get", return_value=Mock(content=SAMPLE_HTML)):
        assert html_dataset.fetch_contents("url") == BeautifulSoup(SAMPLE_HTML, "html.parser")


def test_html_dataset_get_text(html_dataset):
    soup = BeautifulSoup(f"<article>{SAMPLE_CONTENTS}</article>", "html.parser")
    assert html_dataset._get_text(soup) == "bla bla bla [a link](http://ble.com) bla bla"


def test_html_dataset_find_date(html_dataset):
    text = """
    <div>
        <span>Some random thing</span>
        <span>another random thing</span>
        <span>Oct 7, 2023</span>
        <span>more random stuff</span>
        </div>
    </div>
    """
    soup = BeautifulSoup(text, "html.parser")
    assert html_dataset._find_date(soup.select("span")) == parse("2023-10-07T00:00:00Z")


@pytest.mark.parametrize(
    "text",
    (
        SAMPLE_CONTENTS,
        BeautifulSoup(SAMPLE_CONTENTS, "html.parser"),
    ),
)
def test_html_dataset_extract_metadata(html_dataset, text):
    assert html_dataset._extract_markdown(text) == "bla bla bla [a link](http://ble.com) bla bla"


def test_html_dataset_process_entry(html_dataset):
    item = f"""
    <div>
      <h2>the title</h2>
      <a href="{html_dataset.url}/path/to/article">click to read more</a>
    </div>
    """
    article = BeautifulSoup(item, "html.parser")

    with patch("requests.get", return_value=Mock(content=SAMPLE_HTML)):
        assert html_dataset.process_entry(article).to_dict() == {
            "authors": ["John Smith", "Your momma"],
            "date_published": None,
            "id": None,
            "source": "bla",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla [a link](http://ble.com) bla bla",
            "title": "This is the title",
            "url": "http://example.com/path/to/article",
        }


def test_html_dataset_process_entry_no_text(html_dataset):
    item = f'<div><a href="{html_dataset.url}/path/to/article">click to read more</a></div>'
    article = BeautifulSoup(item, "html.parser")

    with patch("requests.get", return_value=Mock(content="")):
        assert html_dataset.process_entry(article) is None


def test_html_dataset_newline_in_title(html_dataset: HTMLDataset):
    html_with_newline_title = f"""
    <article>
        <h1>
            the title\nwith a newline
        </h1>
            <div>
                bla bla bla <a href="{html_dataset.url}/path/to/article">click to read more</a> bla bla
            </div>
    </article>
    """
    article = BeautifulSoup(html_with_newline_title, "html.parser")

    with patch("requests.get", return_value=Mock(content=html_with_newline_title)):
        assert html_dataset.process_entry(article).to_dict() == {
            "authors": ["John Smith", "Your momma"],
            "date_published": None,
            "id": None,
            "source": "bla",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla [click to read more](http://example.com/path/to/article) bla bla",
            "title": "the title with a newline",
            "url": "http://example.com/path/to/article",
        }


@pytest.mark.parametrize(
    "item, authors",
    (
        ({}, ["default author"]),
        ({"bla": 123123}, ["default author"]),
        ({"authors": []}, []),
        ({"authors": [{}, {"bla": "asd"}, {"name": None}, {"name": ""}]}, []),
        (
            {"authors": [{"name": "John Smith"}, {"name": "your momma"}]},
            ["John Smith", "your momma"],
        ),
    ),
)
def test_rss_dataset_extract_authors(item, authors):
    dataset = RSSDataset(name="bla", url="http://example.org", authors=["default author"])
    assert dataset.extract_authors(item) == authors


def test_rss_dataset_get_title():
    assert RSSDataset._get_title({"title": "title"}) == "title"


@pytest.mark.parametrize(
    "item, date",
    (
        ({"published": "2012/01/02 12:32"}, parse("2012-01-02T12:32:00Z")),
        ({"pubDate": "2012/01/02 12:32"}, parse("2012-01-02T12:32:00Z")),
        (
            {
                "pubDate": "2032/01/02 12:32",
                "published": "2012/01/02 12:32",
            },
            parse("2012-01-02T12:32:00Z"),
        ),
        ({"bla": "bla"}, None),
    ),
)
def test_rss_dataset_get_published_date(item, date):
    dataset = RSSDataset(name="bla", url="http://example.org", authors=["default author"])
    assert dataset._get_published_date(item) == date


@pytest.mark.parametrize(
    "item",
    (
        {},
        {"content": None},
        {"content": ""},
        {"content": []},
        {"content": [{}]},
        {"content": [{"bla": "asd"}]},
    ),
)
def test_rss_dataset_get_text_missing(item):
    dataset = RSSDataset(name="bla", url="http://example.org")
    assert not dataset._get_text(item)


def test_rss_dataset_get_text():
    dataset = RSSDataset(name="bla", url="http://example.org")
    assert (
        dataset._get_text({"content": [{"value": SAMPLE_CONTENTS}]})
        == "bla bla bla [a link](http://ble.com) bla bla"
    )


def test_rss_dataset_get_text_ignored_selectors():
    """Feed content honors ignored_selectors, the same way page content does in HTMLDataset."""
    dataset = RSSDataset(name="bla", url="http://example.org")
    dataset.ignored_selectors = ["footer", "div.subscribe"]
    html = (
        "<p>keep this</p>"
        '<div class="subscribe">Subscribe now</div>'
        "<p>and this</p>"
        "<footer>Share this post</footer>"
    )
    assert dataset._get_text({"content": [{"value": html}]}) == "keep this\n\nand this"


def test_rss_dataset_get_text_does_not_change_the_item():
    dataset = RSSDataset(name="bla", url="http://example.org")
    dataset.ignored_selectors = ["footer"]
    html = "<p>keep this</p><footer>Share this post</footer>"
    item = {"content": [{"value": html}]}
    dataset._get_text(item)
    assert item == {"content": [{"value": html}]}


def test_rss_datasetfetch_contents_with_contents():
    dataset = RSSDataset(name="bla", url="http://example.org")
    dataset.items = {"http://bla.bla": {"content": "contents"}}

    assert dataset.fetch_contents("http://bla.bla") == {"content": "contents"}


def test_rss_datasetfetch_contents_no_contents():
    dataset = RSSDataset(name="bla", url="http://example.org")
    dataset.items = {"http://bla.bla": {}}

    contents = "<div>bla</div>"
    with patch("requests.get", return_value=Mock(content=contents)):
        assert dataset.fetch_contents("http://bla.bla") == {
            "soup": BeautifulSoup(contents, "html.parser")
        }


def test_rss_dataset_items_list():
    dataset = RSSDataset(name="bla", url="http://example.org")
    contents = {
        "entries": [
            {
                "link": f"http://example.org/article-{i}",
                "title": f"Article no {i}",
            }
            for i in range(5)
        ]
    }

    with patch("requests.get", return_value=_resp()), patch(
        "feedparser.parse", return_value=contents
    ):
        assert dataset.items_list == [f"http://example.org/article-{i}" for i in range(5)]


# --- Network timeouts and loud feed failures (#254) ---------------------------
#
# `feedparser.parse(url)` fetches through urllib with no timeout, and returns an
# empty feed on a 404 without raising. These pin the fixed behaviour: feeds are
# fetched with requests (with a timeout) and the body handed to feedparser, a bad
# status is loud, transient errors are retried per the CLAUDE.md policy, and every
# other requests.get carries a timeout. Hosts use `.invalid` so that nothing here
# can reach the network even while the implementation is missing.

SAMPLE_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example feed</title>
    <link>http://feed.invalid</link>
    <item>
      <title>Article no 0</title>
      <link>http://feed.invalid/article-0</link>
    </item>
    <item>
      <title>Article no 1</title>
      <link>http://feed.invalid/article-1</link>
    </item>
  </channel>
</rss>
"""
SAMPLE_FEED_LINKS = ["http://feed.invalid/article-0", "http://feed.invalid/article-1"]


def _resp(content=b"", status=200):
    """A fake `requests.Response` whose `raise_for_status` raises on 4xx/5xx like the real one."""
    resp = Mock(content=content, status_code=status)
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"{status} error", response=resp
        )
    return resp


def test_rss_dataset_items_list_fetches_feed_with_timeout():
    """The feed is fetched via requests with a timeout and the body handed to feedparser,
    rather than feedparser fetching the URL itself (urllib, no timeout -- can hang forever)."""
    dataset = RSSDataset(name="bla", url="http://feed.invalid")
    with patch("requests.get", return_value=_resp(SAMPLE_FEED)) as mock_get:
        assert dataset.items_list == SAMPLE_FEED_LINKS

    mock_get.assert_called_once()
    assert mock_get.call_args.args[0] == "http://feed.invalid/rss.xml"
    assert mock_get.call_args.kwargs["timeout"] == settings.REQUEST_TIMEOUT


@patch("align_data.common.retry.time.sleep")
def test_rss_dataset_items_list_raises_on_http_error(mock_sleep):
    """A 404 feed is loud rather than silently empty, and is not retried (it won't self-resolve)."""
    dataset = RSSDataset(name="bla", url="http://feed.invalid")
    with patch("requests.get", return_value=_resp(status=404)) as mock_get:
        with pytest.raises(requests.exceptions.HTTPError):
            dataset.items_list
    assert mock_get.call_count == 1


@patch("align_data.common.retry.time.sleep")
def test_rss_dataset_items_list_retries_transient_errors(mock_sleep):
    """Connection resets and read timeouts are retried, per the CLAUDE.md retry policy."""
    dataset = RSSDataset(name="bla", url="http://feed.invalid")
    with patch(
        "requests.get",
        side_effect=[
            requests.exceptions.ConnectionError("connection reset"),
            requests.exceptions.ReadTimeout("read timed out"),
            _resp(SAMPLE_FEED),
        ],
    ) as mock_get:
        assert dataset.items_list == SAMPLE_FEED_LINKS
    assert mock_get.call_count == 3


@patch("align_data.common.retry.time.sleep")
def test_rss_dataset_items_list_raises_after_persistent_timeout(mock_sleep):
    """If the host stays unreachable the last error propagates instead of the run hanging."""
    dataset = RSSDataset(name="bla", url="http://feed.invalid")
    with patch(
        "requests.get", side_effect=requests.exceptions.ReadTimeout("read timed out")
    ) as mock_get:
        with pytest.raises(requests.exceptions.RequestException):
            dataset.items_list
    assert mock_get.call_count == 3


def test_html_dataset_items_list_passes_timeout(html_dataset):
    with patch("requests.get", return_value=Mock(content="<article></article>")) as mock_get:
        html_dataset.items_list
    assert mock_get.call_args.kwargs["timeout"] == settings.REQUEST_TIMEOUT


def test_html_dataset_fetch_contents_passes_timeout(html_dataset):
    with patch("requests.get", return_value=Mock(content=SAMPLE_HTML)) as mock_get:
        html_dataset.fetch_contents("http://example.com/article")
    assert mock_get.call_args.kwargs["timeout"] == settings.REQUEST_TIMEOUT


def test_rss_dataset_fetch_contents_passes_timeout():
    dataset = RSSDataset(name="bla", url="http://feed.invalid")
    dataset.items = {"http://bla.bla": {}}
    with patch("requests.get", return_value=Mock(content="<div>bla</div>")) as mock_get:
        dataset.fetch_contents("http://bla.bla")
    assert mock_get.call_args.kwargs["timeout"] == settings.REQUEST_TIMEOUT
