import json
import pytest
import jsonlines
from pathlib import Path
from bs4 import BeautifulSoup

from align_data.common.alignment_dataset import DataEntry
from align_data.greaterwrong.greaterwrong import (
    GreaterWrong, extract_author, get_attr, parse_karma, extract_metadata, fetch_month_urls, fetch_all_urls, parse_comments
)


def test_extract_author_with_valid_data():
    base_url = 'http://example.com'
    html = '<a href="/user/12345" data-full-name="John Doe" data-userid="12345">John Doe</a>'
    soup = BeautifulSoup(html, 'html.parser')
    a = soup.find('a')

    expected_result = {
        'fullName': 'John Doe',
        'userId': '12345',
        'userLink': 'http://example.com/user/12345',
        'name': 'John Doe',
    }
    assert extract_author(base_url, a) == expected_result


def test_extract_author_with_missing_data():
    base_url = 'http://example.com'
    html = '<a href="/user/12345">John Doe</a>'
    soup = BeautifulSoup(html, 'html.parser')
    a = soup.find('a')

    expected_result = {
        'fullName': None,
        'userId': None,
        'userLink': 'http://example.com/user/12345',
        'name': 'John Doe',
    }
    assert extract_author(base_url, a) == expected_result


def test_extract_author_with_invalid_data():
    base_url = 'http://example.com'
    html = '<a>John Doe</a>'
    soup = BeautifulSoup(html, 'html.parser')
    a = soup.find('a')

    expected_result = {
        'fullName': None,
        'userId': None,
        'userLink': None,
        'name': 'John Doe',
    }
    assert extract_author(base_url, a) == expected_result


def test_extract_author_with_missing_base_url():
    base_url = None
    html = '<a href="/user/12345" data-full-name="John Doe" data-userid="12345">John Doe</a>'
    soup = BeautifulSoup(html, 'html.parser')
    a = soup.find('a')

    expected_result = {
        'fullName': 'John Doe',
        'userId': '12345',
        'userLink': None,
        'name': 'John Doe',
    }
    with pytest.raises(TypeError):
        extract_author(base_url, a)


@pytest.mark.parametrize('div, selector, attr, expected', (
    # Test basic functionality
    ('div', {'class': 'inner'}, 'id', 'target'),
    ('div', {'class': 'inner'}, 'data-value', '123'),

    # missing tag selects anything
    (None, {'class': 'inner'}, 'id', 'target'),
    (None, {'class': 'non-existent'}, 'id', None),

    # Test missing attribute
    ('div', {'class': 'inner'}, 'non-existent', None),
    ('div', {'class': 'non-existent'}, 'id', None),
))
def test_get_attr(div, selector, attr, expected):
    html = '''
        <div class="outer">
            <div class="inner" id="target" data-value="123">Some text</div>
        </div>
    '''
    soup = BeautifulSoup(html, 'html.parser')
    outer = soup.find('div', {'class': 'outer'})

    assert get_attr(outer, div, selector, attr) == expected


@pytest.mark.parametrize('attr, processor, expected', (
    (None, lambda v: v.text.upper(), 'SOME TEXT'),
    (None, lambda v: 'ble ble ble', 'ble ble ble'),

    ('data-value', int, 123),
    ('class', lambda v: v + ['bla bla'], ['inner', 'bla bla']),
))
def test_get_attr_post_processing(attr, processor, expected):
    html = '''
        <div class="outer">
            <div class="inner" id="target" data-value="123">Some text</div>
        </div>
    '''
    soup = BeautifulSoup(html, 'html.parser')
    outer = soup.find('div', {'class': 'outer'})

    assert get_attr(outer, 'div', {'class': 'inner'}, attr, processor) == expected


@pytest.mark.parametrize('text, expected_karma, expected_score', (
    ('1 point', {'LW': 1}, 1),
    ('432 points', {'LW': 432}, 432),

    ('LW: 32 AF: 42 EA: 12', {'LW': 32, 'AF': 42, 'EA': 12}, 32),
    ('AF: 42 LW: 32 EA: 12', {'LW': 32, 'AF': 42, 'EA': 12}, 42),
))
def test_parse_karma(text, expected_karma, expected_score):
    html = f'''<div>
        <a href="/bla/bla" class="lw2-link" title="LW">Less Wrong</a>
        <span class="karma-value">{text}</span>
    </div>
    '''
    meta_div = BeautifulSoup(html, 'html.parser')
    score, karma = parse_karma(meta_div)
    assert score == expected_score
    assert karma == expected_karma


@pytest.mark.parametrize('html', (
    '<div></div>',
    '''<div>
        <a href="/bla/bla" class="lw2-link" title="LW">Less Wrong</a>
    </div>''',
    '''<div>
        <a href="/bla/bla" class="lw2-link" title="LW">Less Wrong</a>
        <span class="asdasdad"></span>
    </div>''',
))
def test_parse_karma_missing_counts(html):
    # Test case when Karma and site are None
    meta_div = BeautifulSoup(html, 'html.parser')
    score, karma = parse_karma(meta_div)
    assert score == None
    assert karma == {}


def test_extract_metadata():
    html = '''
    <main class="post">
      <h1 class="post-title">Outside the Laboratory</h1>
      <div class="post-meta top-post-meta">
        <a class="author" data-full-name="" data-userid="nmk3nLpQE89dMRzzN" href="/users/eliezer_yudkowsky">Eliezer Yudkowsky</a>
        <span class="date hide-until-init" data-js-date="1169351217000">21 Jan 2007 3:46 UTC <script async="" src="data:text/javascript,prettyDate()"></script></span>
        <div class="karma voting-controls" data-post-id="N2pENnTPB75sfc9kb">
            <button autocomplete="off" class="vote upvote" data-target-type="Post" data-vote-type="upvote" disabled="" tabindex="-1" type="button">
            </button>
            <span class="karma-value" title="108 votes"> 132 <span> points </span></span>
            <button autocomplete="off" class="vote downvote" data-target-type="Post" data-vote-type="downvote" disabled="" tabindex="-1" type="button"> </button>
        </div> <a class="comment-count" href="#comments"> 351 <span> comments </span> </a>
        <a class="lw2-link" href="https://www.lesswrong.com/posts/N2pENnTPB75sfc9kb/outside-the-laboratory"> LW <span> link </span> </a>
        <a class="archive-link" href="https://web.archive.org/web/*/http://lesswrong.com/lw/gv/outside_the_laboratory"> Archive </a>
        <a class="post-section frontpage" href="/" title="View Frontpage posts"> </a>
        <div id="tags">
            <a href="/tag/law-thinking"> Law-Thinking </a>
            <a href="/tag/rationality"> Rationality </a>
            <a href="/tag/practice-and-philosophy-of-science"> Practice &amp; Philosophy of Science </a>
            <a href="/tag/religion"> Religion </a>
            <a href="/tag/compartmentalization"> Compartmentalization </a>
        </div>
        <nav class="qualified-linking">
            <input id="qualified-linking-toolbar-toggle-checkbox-top" tabindex="-1" type="checkbox"/>
            <label for="qualified-linking-toolbar-toggle-checkbox-top"> <span>  </span> </label>
            <div class="qualified-linking-toolbar">
                <a href="/posts/N2pENnTPB75sfc9kb/outside-the-laboratory"> Post permalink </a>
                <a href="/posts/N2pENnTPB75sfc9kb/outside-the-laboratory?comments=false"> Link without comments </a>
                <a href="/posts/N2pENnTPB75sfc9kb/outside-the-laboratory?hide-nav-bars=true"> Link without top nav bars </a>
                <a href="/posts/N2pENnTPB75sfc9kb/outside-the-laboratory?comments=false&amp;hide-nav-bars=true"> Link without comments or top nav bars </a>
            </div>
        </nav>
      </div>
</main>
    '''
    post = BeautifulSoup(html, 'html.parser').find('h1')
    assert extract_metadata('http://bla.bla', post, post.find_next_sibling('div')) == {
        'title': 'Outside the Laboratory',
        'url': 'https://www.lesswrong.com/posts/N2pENnTPB75sfc9kb/outside-the-laboratory',
        'authors': [{
            'fullName': '',
            'userId': 'nmk3nLpQE89dMRzzN',
            'userLink': 'http://bla.bla/users/eliezer_yudkowsky',
            'name': 'Eliezer Yudkowsky'
        }],
        'date_published': '2007-01-21T03:46:00',
        'votes': 108,
        'score': 132,
        'karma': {' LW ': 132},
        'tags': ['Law-Thinking', 'Rationality', 'Practice & Philosophy of Science', 'Religion', 'Compartmentalization']
    }


def test_extract_metadata_remove_empty():
    html = '<main class="post"> <h1 class="post-title">Outside the Laboratory</h1> </main>'
    post = BeautifulSoup(html, 'html.parser').find('h1')
    assert extract_metadata('http://bla.bla', post) == {'title': 'Outside the Laboratory'}
