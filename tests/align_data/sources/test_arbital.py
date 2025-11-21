import pytest
import pytz
from dateutil.parser import parse

from align_data.sources.arbital import arbital as arbital_module
from align_data.sources.arbital.arbital import Arbital, _merge_authors, _extract_summaries


@pytest.fixture
def dataset():
    return Arbital(name="arbital")


def test_headers_require_access_token(monkeypatch, dataset):
    monkeypatch.setattr(arbital_module, "LW_GRAPHQL_ACCESS", None)

    with pytest.raises(ValueError):
        dataset._headers()


def test_headers_include_custom_header(monkeypatch, dataset):
    monkeypatch.setattr(arbital_module, "LW_GRAPHQL_ACCESS", "X-Test-Header: abc123")

    headers = dataset._headers()

    assert headers["Content-Type"] == "application/json"
    assert headers["User-Agent"] == "alignment-research-dataset/arbital-lw"
    assert headers["X-Test-Header"] == "abc123"


def test_merge_authors_dedupes_and_ignores_empty():
    primary = [{"displayName": "Alice"}, {"displayName": "Bob"}, {"displayName": ""}]
    secondary = ["Alice", None, "Charlie"]

    assert _merge_authors(primary, secondary) == ["Alice", "Bob", "Charlie"]


def test_extract_summaries_removes_from_text():
    text = """
    [summary: short overview]
    Body text remains here.
    [summary(Technical): deeper points]
    """

    summaries, cleaned = _extract_summaries(text)

    assert summaries == ["short overview", "deeper points"]
    assert cleaned == "Body text remains here."


@pytest.mark.parametrize(
    "tag, expected",
    (
        ({"description": {"markdown": "md text\n", "plaintextMainText": "fallback"}}, "md text"),
        ({"description": {"plaintextMainText": "plain "}, "description_latest": "latest"}, "plain"),
        ({"description": {}, "description_latest": " latest "}, "latest"),
        ({"description": {}, "description_latest": None}, ""),
    ),
)
def test_choose_text_priority(dataset, tag, expected):
    assert dataset._choose_text(tag) == expected


@pytest.mark.parametrize(
    "tag, expected",
    (
        ({"editCreatedAt": "2025-01-01T00:00:00Z"}, parse("2025-01-01T00:00:00Z").replace(tzinfo=pytz.UTC)),
        ({"pageCreatedAt": "2024-05-06T07:08:09Z"}, parse("2024-05-06T07:08:09Z").replace(tzinfo=pytz.UTC)),
        ({"description": {"editedAt": "2024-01-02T03:04:05Z"}, "textLastUpdatedAt": "2023-01-01T00:00:00Z"}, parse("2024-01-02T03:04:05Z").replace(tzinfo=pytz.UTC)),
        ({"description": {}, "textLastUpdatedAt": "2023-02-03T04:05:06Z"}, parse("2023-02-03T04:05:06Z").replace(tzinfo=pytz.UTC)),
        ({"description": {}, "textLastUpdatedAt": None, "createdAt": "2020-05-06T07:08:09Z"}, parse("2020-05-06T07:08:09Z").replace(tzinfo=pytz.UTC)),
        ({}, None),
    ),
)
def test_get_published_date_order(dataset, tag, expected):
    assert dataset._get_published_date(tag) == expected


def test_extract_authors_merges_and_dedupes(dataset):
    tag = {
        "description": {"user": {"displayName": "Primary"}},
        "contributors": {
            "contributors": [
                {"user": {"displayName": "Helper"}},
                {"user": {"displayName": "Primary"}},
            ]
        },
    }

    assert dataset._extract_authors(tag) == ["Primary", "Helper"]


def test_extract_authors_defaults_to_anonymous(dataset):
    assert dataset._extract_authors({}) == ["anonymous"]


def test_items_list_paginates(monkeypatch, dataset):
    dataset.limit = 2
    dataset.COOLDOWN = 0

    pages = {
        0: {"results": [{"slug": "a"}, {"slug": "b"}], "totalCount": 3},
        2: {"results": [{"slug": "c"}], "totalCount": 3},
    }
    fetch_offsets = []

    def fake_fetch(offset):
        fetch_offsets.append(offset)
        return pages.get(offset, {"results": [], "totalCount": 3})

    monkeypatch.setattr(dataset, "_fetch_page", fake_fetch)

    assert list(dataset.items_list) == [{"slug": "a"}, {"slug": "b"}, {"slug": "c"}]
    assert fetch_offsets == [0, 2]


def test_items_list_skips_duplicates(monkeypatch, dataset):
    dataset.limit = 2
    dataset.COOLDOWN = 0

    pages = {
        0: {"results": [{"slug": "a"}, {"slug": "dup"}], "totalCount": 4},
        2: {"results": [{"slug": "dup"}, {"slug": "b"}], "totalCount": 4},
    }
    def fake_fetch(offset):
        return pages.get(offset, {"results": [], "totalCount": 4})

    monkeypatch.setattr(dataset, "_fetch_page", fake_fetch)

    assert list(dataset.items_list) == [{"slug": "a"}, {"slug": "dup"}, {"slug": "b"}]


def test_get_item_key_prefers_slug(dataset):
    assert dataset.get_item_key({"slug": "tag-slug", "_id": "123"}) == "tag-slug"
    assert dataset.get_item_key({"_id": "fallback"}) == "fallback"


def test_process_entry_returns_article(dataset):
    tag = {
        "name": "Test Tag",
        "slug": "test-tag",
        "description": {
            "markdown": "[summary: overview]\nSome text",
            "editedAt": "2020-01-02T03:04:05Z",
            "user": {"displayName": "Author 1"},
        },
        "contributors": {"contributors": [{"user": {"displayName": "Author 2"}}]},
        "textLastUpdatedAt": "2020-01-01T00:00:00Z",
        "createdAt": "2019-12-31T00:00:00Z",
        "isArbitalImport": True,
        "wikiOnly": False,
        "arbitalLinkedPages": {"parents": ["abc"]},
    }

    entry = dataset.process_entry(tag)

    assert entry is not None

    data = entry.to_dict()
    assert data["title"] == "Test Tag"
    assert data["text"] == "Some text"
    assert data["authors"] == ["Author 1", "Author 2"]
    assert data["url"] == "https://www.lesswrong.com/tag/test-tag"
    assert data["date_published"] == "2020-01-02T03:04:05Z"
    assert data["slug"] == "test-tag"
    assert data["arbital_linked_pages"] == {"parents": ["abc"]}
    assert data["is_arbital_import"] is True
    assert data["wiki_only"] is False
    assert data["summaries"] == ["overview"]


def test_process_entry_skips_when_no_text(dataset):
    tag = {"name": "Empty", "slug": "empty", "description": {"markdown": "   "}}

    assert dataset.process_entry(tag) is None
