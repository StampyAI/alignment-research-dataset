from unittest.mock import patch, Mock

import pytest
from bs4 import BeautifulSoup

from align_data.common.html_dataset import HTMLDataset, RSSDataset


@pytest.fixture
def html_dataset(tmp_path):
    dataset = HTMLDataset(name='bla', url='http://example.com', authors=['John Smith', 'Your momma'])
    dataset.__post_init__(tmp_path)

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

def test_html_dataset_extract_authors(html_dataset):
    assert html_dataset.extract_authors('dummy variable') == ['John Smith', 'Your momma']


def test_html_dataset_get_title(html_dataset):
    item = f"""
    <article>
      <h1>   This is the title
      </h1>
      <a href="{html_dataset.url}/path/to/article">click to read more</a>
    </article>
    """
    soup = BeautifulSoup(item, "html.parser")
    assert html_dataset._get_title(soup) == 'This is the title'


def test_html_dataset_get_title_missing(html_dataset):
    soup = BeautifulSoup('', "html.parser")
    assert html_dataset._get_title(soup) is None


def test_html_dataset_get_item_key(html_dataset):
    item = f"""
    <div>
      <h2>the title</h2>
      <a href="{html_dataset.url}/path/to/article">click to read more</a>
    </div>
    """
    soup = BeautifulSoup(item, "html.parser")
    assert html_dataset.get_item_key(soup) == 'http://example.com/path/to/article'


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
    with patch('requests.get', return_value=Mock(content=text)):
        assert [i.text for i in html_dataset.items_list] == [
            'article 1',
            'article 2',
            'article 3',
            'article 4',
            'article 5',
        ]


def test_html_dataset_get_contents(html_dataset):
    with patch('requests.get', return_value=Mock(content=SAMPLE_HTML)):
        assert html_dataset._get_contents('url') == BeautifulSoup(SAMPLE_HTML, "html.parser")


def test_html_dataset_get_text(html_dataset):
    soup = BeautifulSoup(f'<article>{SAMPLE_CONTENTS}</article>', "html.parser")
    assert html_dataset._get_text(soup) == 'bla bla bla [a link](http://ble.com) bla bla'


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
    assert html_dataset._find_date(soup.select('span')) == '2023-10-07T00:00:00Z'


@pytest.mark.parametrize('text', (
    SAMPLE_CONTENTS,
    BeautifulSoup(SAMPLE_CONTENTS, "html.parser"),
))
def test_html_dataset_extract_metadata(html_dataset, text):
    assert html_dataset._extract_markdown(text) == 'bla bla bla [a link](http://ble.com) bla bla'


def test_html_dataset_process_entry(html_dataset):
    item = f"""
    <div>
      <h2>the title</h2>
      <a href="{html_dataset.url}/path/to/article">click to read more</a>
    </div>
    """
    article = BeautifulSoup(item, "html.parser")

    with patch('requests.get', return_value=Mock(content=SAMPLE_HTML)):
        assert html_dataset.process_entry(article) == {
            'authors': ['John Smith', 'Your momma'],
            'date_published': '',
            'id': None,
            'source': 'bla',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla [a link](http://ble.com) bla bla',
            'title': 'This is the title',
            'url': 'http://example.com/path/to/article',
        }


def test_html_dataset_process_entry_no_text(html_dataset):
    item = f'<div><a href="{html_dataset.url}/path/to/article">click to read more</a></div>'
    article = BeautifulSoup(item, "html.parser")

    with patch('requests.get', return_value=Mock(content='')):
        assert html_dataset.process_entry(article) is None


@pytest.mark.parametrize('item, authors', (
    ({}, ['default author']),
    ({'bla': 123123}, ['default author']),

    ({'authors': []}, []),
    ({'authors': [{}, {'bla': 'asd'}, {'name': None}, {'name': ''}]}, []),

    ({'authors': [{'name': 'John Smith'}, {'name': 'your momma'}]}, ['John Smith', 'your momma']),
))
def test_rss_dataset_extract_authors(item, authors):
    dataset = RSSDataset(name='bla', url='http://example.org', authors=['default author'])
    assert dataset.extract_authors(item) == authors


def test_rss_dataset_get_title():
    assert RSSDataset._get_title({'title': 'title'}) == 'title'


@pytest.mark.parametrize('item, date', (
    ({'published': '2012/01/02 12:32'}, '2012-01-02T12:32:00Z'),
    ({'pubDate': '2012/01/02 12:32'}, '2012-01-02T12:32:00Z'),
    ({
        'pubDate': '2032/01/02 12:32',
        'published': '2012/01/02 12:32',
    }, '2012-01-02T12:32:00Z'),

    ({'bla': 'bla'}, ''),
))
def test_rss_dataset_get_published_date(item, date):
    dataset = RSSDataset(name='bla', url='http://example.org', authors=['default author'])
    assert dataset._get_published_date(item) == date


@pytest.mark.parametrize('item', (
    {},
    {'content': None},
    {'content': ''},

    {'content': []},
    {'content': [{}]},
    {'content': [{'bla': 'asd'}]},
))
def test_rss_dataset_get_text_missing(item):
    dataset = RSSDataset(name='bla', url='http://example.org')
    assert not dataset._get_text(item)


def test_rss_dataset_get_text():
    dataset = RSSDataset(name='bla', url='http://example.org')
    assert dataset._get_text({'content': [{'value': SAMPLE_CONTENTS}]}) == 'bla bla bla [a link](http://ble.com) bla bla'


def test_rss_dataset_get_contents_with_contents():
    dataset = RSSDataset(name='bla', url='http://example.org')
    dataset.items = {
        'http://bla.bla': {
            'content': 'contents'
        }
    }

    assert dataset._get_contents('http://bla.bla') == {'content': 'contents'}


def test_rss_dataset_get_contents_no_contents():
    dataset = RSSDataset(name='bla', url='http://example.org')
    dataset.items = {'http://bla.bla': {}}

    contents = '<div>bla</div>'
    with patch('requests.get', return_value=Mock(content=contents)):
        assert dataset._get_contents('http://bla.bla') == {
            'soup': BeautifulSoup(contents, "html.parser")
        }


def test_rss_dataset_items_list():
    dataset = RSSDataset(name='bla', url='http://example.org')
    contents = {
        'entries': [
            {
                'link': f'http://example.org/article-{i}',
                'title': f'Article no {i}',
            } for i in range(5)
        ]
    }

    with patch('feedparser.parse', return_value=contents):
        assert dataset.items_list == [f'http://example.org/article-{i}' for i in range(5)]
