import pytest
from unittest.mock import patch, Mock

from align_data.db.models import Article
from align_data.sources.validate import update_article_field, check_article, check_articles


@pytest.mark.parametrize('url', (
    None, '',
    'http://blablabla.g', # this isn't longer
    'http://example.org',
    'https://example.org',
    'https://www.example.org',
))
def test_update_article_url_no_replace(url):
    article = Article(url='https://example.org', title='Example')
    update_article_field(article, 'url', url)
    assert article.url == 'https://example.org'


@pytest.mark.parametrize('url', (
    'http://bla.bla.bla.org',
    'https://example.org/ble/ble',
))
def test_update_article_url_replace(url):
    article = Article(url='https://example.com', title='Example')
    update_article_field(article, 'url', url)
    assert article.url == url


@pytest.mark.parametrize('title', (
    None, '',
    'ExAmPlE',
    'Example',
    '       Example    ',
    '       Example\n\n    ',

    '1234567', # This different title is shorter, so won't be changed
))
def test_update_article_title_no_replace(title):
    article = Article(url='https://example.com', title='Example')
    update_article_field(article, 'title', title)
    assert article.title == 'Example'


@pytest.mark.parametrize('title', (
    'ExAmPlEs',
    'Some other title',
))
def test_update_article_title_replace(title):
    article = Article(url='https://example.com', title='Example')
    update_article_field(article, 'title', title)
    assert article.title == title



def test_update_article_field_with_meta():
    article = Article(url='https://example.com', title='Example', meta={'bla': 'ble'})
    update_article_field(article, 'meta', {'bla': 'asd', 'ble': 123})
    assert article.meta == {'bla': 'ble', 'ble': 123}


def test_update_article_field_with_new_field():
    article = Article(url='https://example.com', title='Example')
    update_article_field(article, 'description', 'This is an example article.')
    assert article.description == 'This is an example article.'


def test_update_article_field_with_new_field_and_empty_value():
    article = Article(url='https://example.com', title='Example')
    update_article_field(article, 'description', None)
    assert hasattr(article, 'description') == False


def test_check_article_gets_metadata():
    data = {
        'text': 'bla bla',
        'source_url': 'http://ble.com',
        'url': 'http://pretty.url',
        'authors': ['mr. blobby', 'johhny'],
    }
    article = Article(url='http://this.should.change', meta={})
    with patch('align_data.sources.validate.item_metadata', return_value=data):
        with patch('align_data.sources.validate.fetch', return_value=Mock(status_code=200)):
            check_article(article)
            assert article.to_dict() == {
                'authors': ['mr. blobby', 'johhny'],
                'date_published': None,
                'id': None,
                'source': None,
                'source_type': None,
                'source_url': 'http://ble.com',
                'summaries': [],
                'text': 'bla bla',
                'title': None,
                'url': 'http://this.should.change',
            }


def test_check_article_no_metadata():
    data = {
        'error': 'this failed!',
        'text': 'bla bla',
        'source_url': 'http://ble.com',
        'url': 'http://pretty.url',
        'authors': ['mr. blobby', 'johhny'],
    }
    article = Article(url='http://this.should.not.change', meta={})
    with patch('align_data.sources.validate.item_metadata', return_value=data):
        with patch('align_data.sources.validate.fetch', return_value=Mock(status_code=200)):
            check_article(article)
            assert article.to_dict() == {
                'authors': [],
                'date_published': None,
                'id': None,
                'source': None,
                'source_type': None,
                'summaries': [],
                'text': None,
                'title': None,
                'url': 'http://this.should.not.change',
            }


def test_check_article_bad_url():
    article = Article(url='http://this.should.not.change', meta={})
    with patch('align_data.sources.validate.item_metadata', return_value={}):
        with patch('align_data.sources.validate.fetch', return_value=Mock(status_code=400)):
            check_article(article)
            assert article.status == 'Unreachable url'
