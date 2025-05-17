import pytz
from datetime import timedelta, datetime
from dateutil.parser import parse
from unittest.mock import patch, Mock

import pytest

from align_data.sources.greaterwrong.greaterwrong import (
    get_allowed_tags,
    GreaterWrong,
)


def test_get_allowed_tags():
    # Test alignmentforum (should return empty set)
    assert get_allowed_tags("alignmentforum") == set()

    # Test lesswrong (should return AI tag)
    assert get_allowed_tags("lesswrong") == {"AI"}

    # Test eaforum (should return AI Safety tag)
    assert get_allowed_tags("eaforum") == {"AI Safety"}

    # Test unknown datasource
    with pytest.raises(ValueError):
        get_allowed_tags("unknown")


@pytest.fixture
def dataset(tmp_path):
    return GreaterWrong(
        name="bla",
        base_url="http://example.com",
        start_year=2013,
        min_karma=0,
        af=False,
    )


@pytest.mark.parametrize(
    "tags",
    (
        [{"name": "tag1"}],
        [{"name": "tag1"}, {"name": "other tag"}],
        [{"name": "tag1"}, {"name": "tag2"}],
        [{"name": "tag2"}, {"name": "bla"}],
    ),
)
def test_greaterwrong_tags_ok(dataset, tags):
    # Set up the test with allowed tags
    dataset.allowed_tags = {"tag1", "tag2"}
    dataset.name = "lesswrong"  # for config lookup

    # Should accept posts with required tags
    assert dataset.tags_ok({"tags": tags})


@pytest.mark.parametrize(
    "tags",
    (
        [],
        [{"title": "tag1"}],
        [{"name": "tag3"}, {"name": "tag5"}],
        [{"name": "bla"}],
    ),
)
def test_greaterwrong_tags_ok_missing(dataset, tags):
    # Set up the test with allowed tags
    dataset.allowed_tags = {"tag1", "tag2"}
    dataset.name = "lesswrong"  # for config lookup

    # Should reject posts without required tags
    assert not dataset.tags_ok({"tags": tags})
    
    # Test with bypass_tag_check
    with patch("align_data.sources.greaterwrong.greaterwrong.get_source_config", 
              return_value={"bypass_tag_check": True}):
        assert dataset.tags_ok({"tags": tags})


def test_greaterwrong_get_published_date(dataset):
    assert dataset._get_published_date({"postedAt": "2021/02/01"}) == parse("2021-02-01T00:00:00Z")


def test_greaterwrong_get_published_date_missing(dataset):
    assert dataset._get_published_date({}) == None


def test_items_list_no_previous(dataset):
    dataset.allowed_tags = {"tag1", "tag2"}

    def make_item(date):
        return {
            "htmlBody": f"body {date.isoformat()}",
            "tags": [{"name": "tag1"}],
            "postedAt": date.isoformat(),
        }

    # Pretend that a new post drops every month
    def fetcher(next_date):
        results = []
        date = parse(next_date).replace(tzinfo=pytz.UTC)

        if date < parse("2015-01-01 00:00:00+00:00"):
            # Pretend that graphql returns 3 items at once
            results = [
                make_item(date + timedelta(days=30)),
                make_item(date + timedelta(days=60)),
                make_item(date + timedelta(days=90)),
            ]
        return {"results": results}

    with patch.object(dataset, "fetch_posts", fetcher):
        with patch.object(dataset, "make_query", lambda next_date: next_date):
            assert list(dataset.items_list) == [
                make_item(
                    datetime(dataset.start_year, 1, 1).replace(tzinfo=pytz.UTC)
                    + timedelta(days=i * 30)
                )
                for i in range(1, 28)
            ]


def test_items_list_with_previous_items(dataset):
    dataset.allowed_tags = {"tag1", "tag2"}

    def make_item(date):
        return {
            "htmlBody": f"body {date.isoformat()}",
            "tags": [{"name": "tag1"}],
            "postedAt": date.isoformat(),
        }

    # Pretend that a new post drops every month
    def fetcher(next_date):
        results = []
        date = parse(next_date).replace(tzinfo=pytz.UTC)

        if date < parse("2015-01-01 00:00:00+00:00"):
            # Pretend that graphql returns 3 items at once
            results = [
                make_item(date + timedelta(days=30)),
                make_item(date + timedelta(days=60)),
                make_item(date + timedelta(days=90)),
            ]
        return {"results": results}

    mock_items = (i for i in [Mock(date_published=datetime.fromisoformat("2014-12-12T01:23:45"))])
    with patch.object(dataset, "fetch_posts", fetcher):
        with patch.object(dataset, "make_query", lambda next_date: next_date):
            with patch.object(dataset, "read_entries", return_value=mock_items):
                # All items that are older than the newest item in the jsonl file are ignored
                assert list(dataset.items_list) == [
                    make_item(
                        datetime(2014, 12, 12, 1, 23, 45).replace(tzinfo=pytz.UTC)
                        + timedelta(days=i * 30)
                    )
                    for i in range(1, 4)
                ]


def test_process_entry(dataset):
    entry = {
        "coauthors": [{"displayName": "John Snow"}, {"displayName": "Mr Blobby"}],
        "user": {"displayName": "Me"},
        "title": "The title",
        "pageUrl": "http://example.com/bla",
        "modifiedAt": "2001-02-10",
        "postedAt": "2012/02/01 12:23:34",
        "htmlBody": '\n\n bla bla <a href="bla.com">a link</a>    ',
        "voteCount": 12,
        "baseScore": 32,
        "tags": [{"name": "tag1"}, {"name": "tag2"}],
        "wordCount": 123,
        "commentCount": 423,
    }
    assert dataset.process_entry(entry).to_dict() == {
        "authors": ["Me", "John Snow", "Mr Blobby"],
        "comment_count": 423,
        "date_published": "2012-02-01T12:23:34Z",
        "id": None,
        "karma": 32,
        "modified_at": "2001-02-10",
        "source": "bla",
        "source_type": "GreaterWrong",
        "summaries": [],
        "tags": ["tag1", "tag2"],
        "text": "bla bla [a link](bla.com)",
        "title": "The title",
        "url": "http://example.com/bla",
        "votes": 12,
        "words": 123,
    }


def test_process_entry_no_authors(dataset):
    entry = {
        "coauthors": [],
        "user": {},
        "title": "The title",
        "pageUrl": "http://example.com/bla",
        "modifiedAt": "2001-02-10",
        "postedAt": "2012/02/01 12:23:34",
        "htmlBody": '\n\n bla bla <a href="bla.com">a link</a>    ',
        "voteCount": 12,
        "baseScore": 32,
        "tags": [{"name": "tag1"}, {"name": "tag2"}],
        "wordCount": 123,
        "commentCount": 423,
    }
    assert dataset.process_entry(entry).to_dict() == {
        "authors": ["anonymous"],
        "comment_count": 423,
        "date_published": "2012-02-01T12:23:34Z",
        "id": None,
        "karma": 32,
        "modified_at": "2001-02-10",
        "source": "bla",
        "source_type": "GreaterWrong",
        "summaries": [],
        "tags": ["tag1", "tag2"],
        "text": "bla bla [a link](bla.com)",
        "title": "The title",
        "url": "http://example.com/bla",
        "votes": 12,
        "words": 123,
    }


@pytest.mark.parametrize(
    "item",
    (
        {
            # non seen url
            "pageUrl": "http://bla.bla",
            "title": "new item",
            "coauthors": [{"displayName": "your momma"}],
        },
        {
            # already seen title, but different authors
            "title": "this has already been seen",
            "pageUrl": "http://bla.bla",
            "coauthors": [{"displayName": "your momma"}],
        },
        {
            # new title, but same authors
            "coauthors": [{"displayName": "johnny"}],
            "title": "new item",
            "pageUrl": "http://bla.bla",
        },
    ),
)
def test_not_processed_true(item, dataset):
    dataset._outputted_items = ({"http://already.seen"}, {("this has been seen", "johnny")})
    item["user"] = None
    assert dataset.not_processed(item)


@pytest.mark.parametrize(
    "item",
    (
        {
            # url seen
            "pageUrl": "http://already.seen",
            "title": "new item",
            "coauthors": [{"displayName": "your momma"}],
        },
        {
            # already seen title and authors pair, but different url
            "title": "this has already been seen",
            "coauthors": [{"displayName": "johnny"}],
            "pageUrl": "http://bla.bla",
        },
        {
            # already seen everything
            "pageUrl": "http://already.seen",
            "title": "this has already been seen",
            "coauthors": [{"displayName": "johnny"}],
        },
    ),
)
def test_not_processed_false(item, dataset):
    dataset._outputted_items = ({"http://already.seen"}, {("this has already been seen", "johnny")})
    item["user"] = None
    assert not dataset.not_processed(item)
