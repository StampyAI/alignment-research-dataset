from unittest.mock import Mock, patch
from csv import DictWriter
from numpy import source

import pandas as pd
import pytest
from align_data.db.models import Article
from align_data.sources.articles.updater import ReplacerDataset, Item

SAMPLE_UPDATES = [
    {},
    {'title': 'no id - should be ignored'},

    {'id': '122', 'hash_id': 'deadbeef000'},
    {
        'id': '123', 'hash_id': 'deadbeef001',
        'title': 'bla bla',
        'url': 'http://bla.com',
        'source_url': 'http://bla.bla.com',
        'authors': 'mr. blobby, johnny',
    }, {
        'id': '124',
        'title': 'no hash id',
        'url': 'http://bla.com',
        'source_url': 'http://bla.bla.com',
        'authors': 'mr. blobby',
    }, {
        'hash_id': 'deadbeef002',
        'title': 'no id',
        'url': 'http://bla.com',
        'source_url': 'http://bla.bla.com',
        'authors': 'mr. blobby',
    }, {
        'id': '125',
        'title': 'no hash id, url or title',
        'authors': 'mr. blobby',
    }
]

@pytest.fixture
def csv_file(tmp_path):
    filename = tmp_path / 'data.csv'
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['id', 'hash_id', 'title', 'url', 'source_url', 'authors']
        writer = DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in SAMPLE_UPDATES:
            writer.writerow(row)
    return filename


def test_items_list(csv_file):
    dataset = ReplacerDataset(name='bla', csv_path=csv_file, delimiter=',')

    def mock_entries():
        return [
            Mock(
                _id=dataset.maybe(v, 'id'),
                id=dataset.maybe(v, 'hash_id'),
                title=dataset.maybe(v, 'title'),
                url=dataset.maybe(v, 'url'),
                authors=dataset.maybe(v, 'authors')
            )
            for v in dataset.csv_items
        ]

    with patch.object(dataset, 'read_entries', mock_entries):
        items = dataset.items_list
        assert len(items) == 5, "items_list should only contain items with valid ids - something is wrong"
        for item in items:
            assert dataset.maybe(item.updates, 'id') == item.article._id
            assert dataset.maybe(item.updates, 'hash_id') == item.article.id
            assert dataset.maybe(item.updates, 'title') == item.article.title
            assert dataset.maybe(item.updates, 'url') == item.article.url
            assert dataset.maybe(item.updates, 'authors') == item.article.authors


@pytest.mark.parametrize('updates', (
    Mock(url='http://some.other.url'),
    Mock(source_url='http://some.other.url'),
    Mock(url='http://some.other.url', source_url='http://another.url'),
))
def test_update_text(csv_file, updates):
    dataset = ReplacerDataset(name='bla', csv_path=csv_file, delimiter=',')

    article = Mock(text='this should be changed', status='as should this', url='http:/bla.bla.com')

    with patch('align_data.sources.articles.updater.item_metadata', return_value={'text': 'bla bla bla'}):
        dataset.update_text(updates, article)
        assert article.text == 'bla bla bla'
        assert article.status == None


@pytest.mark.parametrize('updates', (
    Mock(url='http://some.other.url'),
    Mock(source_url='http://some.other.url'),
    Mock(url='http://some.other.url', source_url='http://another.url'),
))
def test_update_text_error(csv_file, updates):
    dataset = ReplacerDataset(name='bla', csv_path=csv_file, delimiter=',')

    article = Mock(text='this should not be changed', status='but this should be', url='http:/bla.bla.com')

    with patch('align_data.sources.articles.updater.item_metadata', return_value={'error': 'oh noes!'}):
        dataset.update_text(updates, article)
        assert article.text == 'this should not be changed'
        assert article.status == 'oh noes!'


@pytest.mark.parametrize('updates', (
    Mock(url='http://bla.bla.com', source_url=None, comment='Same url as article, no source_url'),
    Mock(url='http://bla.bla.com', source_url='', comment='Same url as article, empty source_url'),
    Mock(url=None, source_url=None, comment='no urls provided'),
))
def test_update_text_no_update(csv_file, updates):
    dataset = ReplacerDataset(name='bla', csv_path=csv_file, delimiter=',')

    article = Mock(text='this should not be changed', status='as should not this', url='http://bla.bla.com')

    with patch('align_data.sources.articles.updater.item_metadata', return_value={'text': 'bla bla bla'}):
        dataset.update_text(updates, article)
        assert article.text == 'this should not be changed'
        assert article.status == 'as should not this'


def test_process_entry(csv_file):
    dataset = ReplacerDataset(name='bla', csv_path=csv_file, delimiter=',')

    article = Article(
        _id=123, id='deadbeef0123',
        title='this should be changed',
        url='this should be changed',
        text='this should be changed',
        authors='this should be changed',
        date_published='this should be changed',
    )

    updates = Mock(
        id='123',
        hash_id='deadbeef001',
        title='bla bla',
        url='http://bla.com',
        source_url='http://bla.bla.com',
        source='tests',
        authors='mr. blobby, johnny',
        date_published='2000-12-23T10:32:43Z',
    )

    with patch('align_data.sources.articles.updater.item_metadata', return_value={'text': 'bla bla bla'}):
        assert dataset.process_entry(Item(updates, article)).to_dict() == {
            'authors': ['mr. blobby', 'johnny'],
            'date_published': '2000-12-23T10:32:43Z',
            'id': 'd8d8cad8d28739a0862654a0e6e8ce6e',
            'source': 'tests',
            'source_type': None,
            'summaries': [],
            'text': 'bla bla bla',
            'title': 'bla bla',
            'url': 'http://bla.com',
            'source_url': 'http://bla.bla.com',
        }


def test_process_entry_empty(csv_file):
    dataset = ReplacerDataset(name='bla', csv_path=csv_file, delimiter=',')

    article = Article(
        _id=123, id='deadbeef0123',
        title='this should not be changed',
        url='this should not be changed',
        source='this should not be changed',
        authors='this should not be changed',

        text='this should be changed',
        date_published='this should be changed',
    )

    updates = Mock(
        id='123',
        hash_id='deadbeef001',
        title=None,
        url='',
        source_url='http://bla.bla.com',
        source='       ',
        authors=' \n \n \t \t ',
        date_published='2000-12-23T10:32:43Z',
    )

    with patch('align_data.sources.articles.updater.item_metadata', return_value={'text': 'bla bla bla'}):
        assert dataset.process_entry(Item(updates, article)).to_dict() == {
            'authors': ['this should not be changed'],
            'date_published': '2000-12-23T10:32:43Z',
            'id': '606e9224254f508d297bcb17bcc6d104',
            'source': 'this should not be changed',
            'source_type': None,
            'summaries': [],
            'text': 'bla bla bla',
            'title': 'this should not be changed',
            'url': 'this should not be changed',
            'source_url': 'http://bla.bla.com',
        }
