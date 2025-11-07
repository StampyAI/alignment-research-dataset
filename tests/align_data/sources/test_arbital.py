import json
from unittest.mock import Mock, patch

import pytest
from dateutil.parser import parse

from align_data.sources.arbital.arbital import (
    Arbital,
    extract_text,
    flatten,
    parse_arbital_link,
)


@pytest.mark.parametrize(
    "contents, expected",
    (
        ("123", "[https://arbital.com/p/123](https://arbital.com/p/123)"),
        ("123 Some title", "[Some title](https://arbital.com/p/123)"),
        (
            "123 Some title with multiple words",
            "[Some title with multiple words](https://arbital.com/p/123)",
        ),
        ("https://www.gwern.net/ Gwern Branwen", "[Gwern Branwen](https://www.gwern.net/)"),
        ("toc:", "toc:"),  # `toc:` is a mysterious thing
    ),
)
def test_parse_arbital_link(contents, expected):
    assert parse_arbital_link(contents) == expected


@pytest.mark.parametrize(
    "input, expected",
    (
        ([1, 2, 3], [1, 2, 3]),
        ([1, [2, [3], 4]], [1, 2, 3, 4]),
        ((1, (2, 3), 4), [1, 2, 3, 4]),
        ([], []),
        ([5], [5]),
        ([1, "a", [2, ["b"], 3]], [1, "a", 2, "b", 3]),
        ([1, None, [2, [3], None]], [1, None, 2, 3, None]),
    ),
)
def test_flatten(input, expected):
    assert flatten(input) == expected


@pytest.mark.parametrize(
    "text",
    (
        "" "asdasd asd asd as",
        "Stuff that is in parenthesizes (like this) should be left alone"
        "Markdown links [like this](https://bla.bla.com) should not be changed",
    ),
)
def test_markdownify_text_contents_basic_markdown(text):
    _, result = extract_text(text)
    assert result == text


@pytest.mark.parametrize(
    "text, expected",
    (
        (
            "Arbital links [123 like this] should be transformed",
            "Arbital links [like this](https://arbital.com/p/123) should be transformed",
        ),
        ("[summary: summaries should be removed] bla bla bla", "bla bla bla"),
        (
            "    \n \t \n contents get stripped of whitespace    \t \n",
            "contents get stripped of whitespace",
        ),
        (
            "malformed [links](http://bla.bla are handled somewhat",
            "malformed [links](http://bla.bla) are handled somewhat",
        ),
    ),
)
def test_markdownify_text_contents_arbital_markdown(text, expected):
    _, result = extract_text(text)
    assert result == expected


@pytest.mark.parametrize(
    "text, expected",
    (
        (
            "[summary: summaries should be extracted] bla bla bla",
            (["summary: summaries should be extracted"], "bla bla bla"),
        ),
        (
            "[summary: summaries should be extracted] [summary(Technical): technical summary should be handled separately] bla bla bla",
            (["summary: summaries should be extracted", "summary(Technical): technical summary should be handled separately"], "bla bla bla"),
        ),
        (
            "[summary: summaries should be extracted] bla bla bla [summary(Technical): summaries should work in the middle too] bla bla bla",
            (["summary: summaries should be extracted", "summary(Technical): summaries should work in the middle too"], "bla bla bla  bla bla bla"),
        ),
        (
            "[summary: \n    whitespace should be stripped       \n] bla bla bla",
            (["summary: whitespace should be stripped"], "bla bla bla"),
        ),
        (
            "[summary(Bold): special summaries should be extracted] bla bla bla",
            (["summary(Bold): special summaries should be extracted"], "bla bla bla"),
        ),
        (
            "[summary(Markdown): special summaries should be extracted] bla bla bla",
            (["summary(Markdown): special summaries should be extracted"], "bla bla bla"),
        ),
        (
            "[summary(BLEEEE): special summaries should be extracted] bla bla bla",
            (["summary(BLEEEE): special summaries should be extracted"], "bla bla bla"),
        ),
        (
            "[summary: markdown is handled: [bla](https://bla.bla)] bla bla bla",
            (["summary: markdown is handled: [bla](https://bla.bla)"], "bla bla bla"),
        ),
        (
            "[summary: markdown is handled: [123 ble ble]] bla bla bla",
            (["summary: markdown is handled: [ble ble](https://arbital.com/p/123)"], "bla bla bla"),
        ),
    ),
)
def test_markdownify_text_summary_and_content(text, expected):
    summaries, text = extract_text(text)
    assert summaries == expected[0]
    assert text == expected[1]


@pytest.fixture
def dataset():
    dataset = Arbital(name="arbital")
    dataset.titles_map = {}

    def post(url, *args, **kwargs):
        response = Mock()
        response.status_code = 200
        page = json.loads(kwargs.get("data", "{}")).get("pageAlias")

        if "json/explore" in url:
            response.json.return_value = {"pages": {f"{page}-{i}": i for i in range(10)}}
        elif "json/primaryPage" in url:
            response.json.return_value = {
                "pages": {
                    page: {
                        "title": f"{page}-title",
                    }
                }
            }
        else:
            response.json.return_value = {}
        return response

    with patch("requests.post", post):
        yield dataset


def test_items_list(dataset):
    assert dataset.items_list == [
        f"{page}-{i}" for page in dataset.ARBITAL_SUBSPACES for i in range(10)
    ]


def test_get_title_no_items(dataset):
    assert dataset.get_title("bla") == "bla-title"


def test_get_title_cached(dataset):
    dataset.titles_map["bla"] = "ble ble ble"
    assert dataset.get_title("bla") == "ble ble ble"


@pytest.mark.parametrize(
    "side_effect, return_value",
    (
        # The request was successful but no title present
        (None, {"pages": {"bla": {}}}),
        # The request was successful but the title is empty
        (None, {"pages": {"bla": {"title": ""}}}),
        # The request failed
        (ValueError("Oh noes!!"), None),
    ),
)
def test_get_title_error(dataset, side_effect, return_value):
    titles_map = {"a random entry": "to check that nothing gets changed"}
    dataset.titles_map = titles_map

    with patch("requests.post", return_value=return_value, side_effect=side_effect):
        assert dataset.get_title("bla") is None
        # Make sure that errors don't change the titles map
        assert dataset.titles_map == titles_map


def test_extract_authors(dataset):
    authors = ["John Snow", "mr. blobby"]

    def post(url, data, **kwargs):
        pageAlias = json.loads(data).get("pageAlias")
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"pages": {pageAlias: {"title": pageAlias}}}
        return resp

    with patch("requests.post", post):
        page = {"changeLogs": [{"userId": author} for author in authors]}
        assert sorted(dataset.extract_authors(page)) == sorted(authors)


def test_extract_authors_ignore_missing(dataset):
    authors = ["", None, "John Snow", None, None, "mr. blobby", "", ""]
    page = {"changeLogs": [{"userId": author} for author in authors]}

    with patch.object(dataset, "get_title", lambda author: author):
        assert sorted(dataset.extract_authors(page)) == sorted(["John Snow", "mr. blobby"])


@pytest.mark.parametrize(
    "page, expected",
    (
        ({"editCreatedAt": "2021-02-01T01:23:45Z"}, parse("2021-02-01T01:23:45Z")),
        ({"pageCreatedAt": "2021-02-01T01:23:45Z"}, parse("2021-02-01T01:23:45Z")),
        (
            {
                "editCreatedAt": "2021-02-01T01:23:45Z",
                "pageCreatedAt": "2024-02-01T01:23:45Z",
            },
            parse("2021-02-01T01:23:45Z"),
        ),
        ({}, None),
        ({"bla": "asdasd"}, None),
    ),
)
def test_get_published_date(dataset, page, expected):
    assert dataset._get_published_date(page) == expected


def test_process_entry(dataset):
    page = {
        "title": "test article",
        "text": "bla bla bla",
        "editCreatedAt": "2001-02-03T12:34:45Z",
        "alias": "blee",
        "tagIds": [],
    }
    with patch.object(dataset, "get_page", return_value=page):
        assert dataset.process_entry("bla").to_dict() == {
            "alias": "bla",
            "authors": [],
            "date_published": "2001-02-03T12:34:45Z",
            "id": None,
            "source": "arbital",
            "source_type": "text",
            "summaries": [],
            "tags": [],
            "text": "bla bla bla",
            "title": "test article",
            "url": "https://arbital.com/p/blee",
        }
