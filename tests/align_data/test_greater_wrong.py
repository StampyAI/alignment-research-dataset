import pytz
from datetime import timedelta, datetime
from dateutil.parser import parse
from unittest.mock import patch, Mock

import pytest

from align_data.greaterwrong.greaterwrong import (
    fetch_LW_tags, fetch_ea_forum_topics, GreaterWrong
)


def test_fetch_LW_tags():
    contents = """
    <div class="TagPage-description">
      <div class="table">
        <a href="/tag/tag1">tag1</a>
        <a href="/tag/tag2">tag2</a>
        <a href="/tag/tag3">tag3</a>
        <a href="/ignore/this">ignored</a>
      </div>
    </div>
    """
    with patch('requests.get', return_value=Mock(content=contents)):
        assert fetch_LW_tags('http://url.com') == {'tag3', 'tag2', 'tag1'}


def test_fetch_ea_forum_topics():
    contents = """
    <div class="SidebarSubtagsBox-root">
        <a href="/topics/tag1">tag1</a>
        <a href="/topics/tag2">tag2</a>
        <a href="/topics/tag3">tag3</a>
        <a href="/ignore/this">ignored</a>
    </div>
    """
    with patch('requests.get', return_value=Mock(content=contents)):
        assert fetch_ea_forum_topics('http://url.com') == {'tag3', 'tag2', 'tag1'}


@pytest.fixture
def dataset(tmp_path):
    dataset = GreaterWrong(name='bla', base_url='http://example.com', start_year=2013, min_karma=0, af=False)
    dataset.__post_init__(tmp_path)
    return dataset


@pytest.mark.parametrize('tags', (
    [{'name': 'tag1'}],
    [{'name': 'tag1'}, {'name': 'other tag'}],
    [{'name': 'tag1'}, {'name': 'tag2'}],
    [{'name': 'tag2'}, {'name': 'bla'}],
))
def test_greaterwrong_tags_ok(dataset, tags):
    dataset.ai_tags = {'tag1', 'tag2'}
    assert dataset.tags_ok({'tags': tags})


@pytest.mark.parametrize('tags', (
    [],
    [{'title': 'tag1'}],
    [{'name': 'tag3'}, {'name': 'tag5'}],
    [{'name': 'bla'}],
))
def test_greaterwrong_tags_ok_missing(dataset, tags):
    dataset.ai_tags = {'tag1', 'tag2'}
    assert not dataset.tags_ok({'tags': tags})


def test_greaterwrong_get_item_key(dataset):
    assert dataset.get_item_key({'pageUrl': 'item key'}) == 'item key'


def test_greaterwrong_get_published_date(dataset):
    assert dataset._get_published_date({'postedAt': '2021/02/01'}) == '2021-02-01T00:00:00Z'


def test_greaterwrong_get_published_date_missing(dataset):
    assert dataset._get_published_date({}) == ''


def test_items_list_no_previous(dataset):
    dataset.ai_tags = {'tag1', 'tag2'}

    def make_item(date):
        return {
            'htmlBody': f'body {date.isoformat()}', 'tags': [{'name': 'tag1'}],
            'postedAt': date.isoformat()
        }

    # Pretend that a new post drops every month
    def fetcher(next_date):
        results = []
        date = parse(next_date).replace(tzinfo=pytz.UTC)

        if date < parse('2015-01-01 00:00:00+00:00'):
            # Pretend that graphql returns 3 items at once
            results = [
                make_item(date + timedelta(days=30)),
                make_item(date + timedelta(days=60)),
                make_item(date + timedelta(days=90)),
            ]
        return {'results': results}

    with patch.object(dataset, 'fetch_posts', fetcher):
        with patch.object(dataset, 'make_query', lambda next_date: next_date):
            assert list(dataset.items_list) == [
                make_item(datetime(dataset.start_year, 1, 1).replace(tzinfo=pytz.UTC) + timedelta(days=i*30))
                for i in range(1, 28)
            ]


def test_items_list_with_previous_items(dataset):
    dataset.ai_tags = {'tag1', 'tag2'}
    with open(dataset.jsonl_path, 'w') as f:
        f.write('{"date_published": "2014-12-12T01:23:45Z"}\n')

    def make_item(date):
        return {
            'htmlBody': f'body {date.isoformat()}', 'tags': [{'name': 'tag1'}],
            'postedAt': date.isoformat()
        }

    # Pretend that a new post drops every month
    def fetcher(next_date):
        results = []
        date = parse(next_date).replace(tzinfo=pytz.UTC)

        if date < parse('2015-01-01 00:00:00+00:00'):
            # Pretend that graphql returns 3 items at once
            results = [
                make_item(date + timedelta(days=30)),
                make_item(date + timedelta(days=60)),
                make_item(date + timedelta(days=90)),
            ]
        return {'results': results}

    with patch.object(dataset, 'fetch_posts', fetcher):
        with patch.object(dataset, 'make_query', lambda next_date: next_date):
            # All items that are older than the newest item in the jsonl file are ignored
            assert list(dataset.items_list) == [
                make_item(datetime(2014, 12, 12, 1, 23, 45).replace(tzinfo=pytz.UTC) + timedelta(days=i*30))
                for i in range(1, 4)
            ]


def test_process_entry(dataset):
    entry = {
        'coauthors': [{'displayName': 'John Snow'}, {'displayName': 'Mr Blobby'}],
        'user': {'displayName': 'Me'},
        'title': 'The title',
        'pageUrl': 'http://example.com/bla',
        'modifiedAt': '2001-02-10',
        'postedAt': '2012/02/01 12:23:34',
        'htmlBody': '\n\n bla bla <a href="bla.com">a link</a>    ',
        'voteCount': 12,
        'baseScore': 32,
        'tags': [{'name': 'tag1'}, {'name': 'tag2'}],
        'wordCount': 123,
        'commentCount': 423,
    }
    assert dataset.process_entry(entry) == {
        'authors': ['Me', 'John Snow', 'Mr Blobby'],
        'comment_count': 423,
        'date_published': '2012-02-01T12:23:34Z',
        'id': None,
        'karma': 32,
        'modified_at': '2001-02-10',
        'source': 'bla',
        'source_type': 'GreaterWrong',
        'summary': [],
        'tags': ['tag1', 'tag2'],
        'text': 'bla bla [a link](bla.com)',
        'title': 'The title',
        'url': 'http://example.com/bla',
        'votes': 12,
        'words': 123,
    }
