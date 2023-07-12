from unittest.mock import patch, Mock

import pytest
from bs4 import BeautifulSoup

from align_data.blogs import CaradoMoe, ColdTakes, GenerativeInk, GwernBlog, MediumBlog, SubstackBlog, WordpressBlog

import feedparser

SAMPLE_HTML = """
<div>
  bla bla bla <a href="http://ble.com">a link</a> bla bla
</div>
"""

def test_cold_takes_published_date():
    dataset = ColdTakes(
        name="cold_takes",
        url="https://www.cold-takes.com/",
        authors=['Holden Karnofsky'],
    )

    contents = """
    <article>
        <header>
            <time datetime="2001-02-03"></time>
        </header>
    </article>
    """
    soup = BeautifulSoup(contents, "html.parser")
    assert dataset._get_published_date(soup) == '2001-02-03T00:00:00Z'


def test_cold_takes_process_entry():
    dataset = ColdTakes(
        name="cold_takes",
        url="https://www.cold-takes.com/",
        authors=['Holden Karnofsky'],
    )

    item = """
    <article class="feed post tag-implicationsofmostimportantcentury no-image">
      <h2 class="feed-title">How major governments can help with the most important century</h2>
      <a href="/how-governments-can-help-with-the-most-important-century/"></a>
    </article>
    """
    article = """
    <article class="single post featured">
        <header class="single-header kg-canvas">
                <div class="single-meta">
                <span class="single-meta-item single-meta-date">
                        <time datetime="2023-02-28">
                            Feb 28, 2023
                        </time>
                    </span>
                </div>
            <h1 class="single-title">What does Bing Chat tell us about AI risk?</h1>
        </header>
        <div class="single-content kg-canvas u-text-format">
        bla bla bla
        <div style="display:flex; justify-content:center; margin: 0 auto;">
        This is where the social media links will be
        </div>
        <center>Here are disquis comments</center>
        <footer>This is the footer </footer>
    </article>
    """

    with patch('requests.get', return_value=Mock(content=article)):
        assert dataset.process_entry(BeautifulSoup(item, "html.parser")) == {
            'authors': ['Holden Karnofsky'],
            'date_published': '2023-02-28T00:00:00Z',
            'id': None,
            'source': 'cold_takes',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla',
            'title': 'What does Bing Chat tell us about AI risk?',
            'url': 'https://www.cold-takes.com/how-governments-can-help-with-the-most-important-century/',
        }


GENERITIVE_INK_HTML = """
<main>
    <article>
    <h1 class="post-title">
        <a href="/posts/anomalous-tokens-reveal-the-original-identities-of-instruct-models/">Anomalous tokens reveal the original identities of Instruct models</a>
    </h1>
    bla bla bla
    </article>
    <div class="post-info">
        <p></svg>2338 Words</p>
        <p>Feb 9, 2023</p>
    </div>
</main>
"""

def test_generative_ink_published_date():
    dataset = GenerativeInk(
        name="generative.ink",
        url="https://generative.ink/posts/",
        authors=['janus'],
    )

    soup = BeautifulSoup(GENERITIVE_INK_HTML, "html.parser")
    assert dataset._get_published_date(soup) == '2023-02-09T00:00:00Z'


def test_generative_ink_process_entry():
    dataset = GenerativeInk(
        name="generative.ink",
        url="https://generative.ink/posts/",
        authors=['janus'],
    )

    item = """
    <div class="post on-list">
      <div>
        <span class="post-title"><a href="/posts/simulators/"><strong>Simulators</strong></a></span>
        <span class="post-day" style="float:right">Sep 2</span>
      </div>
    </div>
    """
    with patch('requests.get', return_value=Mock(content=GENERITIVE_INK_HTML)):
        assert dataset.process_entry(BeautifulSoup(item, "html.parser")) == {
            'authors': ['janus'],
            'date_published': '2023-02-09T00:00:00Z',
            'id': None,
            'source': 'generative.ink',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla',
            'title': 'Anomalous tokens reveal the original identities of Instruct models',
            'url': 'https://generative.ink/posts/simulators/',
        }


def test_caradomoe_text():
    dataset = CaradoMoe(
        name="carado.moe",
        url='https://carado.moe',
        authors=['Tamsin Leake'],
    )
    contents = f"""
    <div>
        <p class="postmeta"></p>"
        {SAMPLE_HTML}
    </div>
    """
    soup = BeautifulSoup(contents, "html.parser")
    assert dataset._get_text({'soup': soup}) == 'bla bla bla [a link](http://ble.com) bla bla'


def test_caradomoe_process_entry():
    dataset = CaradoMoe(
        name="carado.moe",
        url='https://carado.moe',
        authors=['Tamsin Leake'],
    )
    item = {
        'pubDate': 'Sat, 10 Jun 2023 07:00:00 -0000',
        'title': 'the title',
        'link': 'http://example.com/bla'
    }
    dataset.items = {item['link']: item}
    contents = f"""
    <div>
        <p class="postmeta"></p>"
        {SAMPLE_HTML}
    </div>
    """
    with patch('requests.get', return_value=Mock(content=contents)):
        assert dataset.process_entry(item['link']) == {
            'authors': ['Tamsin Leake'],
            'date_published': '2023-06-10T07:00:00Z',
            'id': None,
            'source': 'carado.moe',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla [a link](http://ble.com) bla bla',
            'title': 'the title',
            'url': 'http://example.com/bla'
        }


GWERN_CONTENTS = f"""
<main>
  <header>
    <h1>The title of the article</h1>
  </header>
  <article>
    <div id="page-metadata">
       <div class="page-metadata-fields">
          <span class="page-date-range">
            <span class="page-creation" title="Date page contents were begun.">
             <em>2021-02-03</em>
            </span>
           –
          <span class="page-source">
            <span class="page-modified" title="Date of last major modification to this page.">
             <em>2023-01-01</em>
            </span>
          </span>
        </span>
      </div>
    </div>
    <div id="markdownBody">
        {SAMPLE_HTML}
    </div>
  </article>
</main>
"""
def test_gwern_get_text():
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])

    soup = BeautifulSoup(GWERN_CONTENTS, "html.parser")
    assert dataset._get_text(soup) == 'bla bla bla [a link](http://ble.com) bla bla'


@pytest.mark.parametrize('metadata, date', (
    ({'modified': '2022-01-02'}, '2022-01-02T00:00:00Z'),
    ({'created': '2022-01-02'}, '2022-01-02T00:00:00Z'),
    ({'created': '2000-01-01', 'modified': '2022-01-02'}, '2022-01-02T00:00:00Z'),

    ({}, ''),
    ({'bla': 'asda'}, '')
))
def test_gwern_get_published_date(metadata, date):
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])

    assert dataset._get_published_date(metadata) == date


def test_gwern_get_article():
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])
    with patch('requests.get', return_value='article contents'):
        assert dataset._get_article('http://bla.com') == 'article contents'


def test_gwern_get_metadata():
    text = """
    ---
    title: "The Scaling Hypothesis"
    thumbnail: /doc/ai/nn/transformer/gpt/2020-brown-gpt3-figure13-meanperformancescalingcurve.png
    created: 2020-05-28
    modified: 2022-01-02
    status: finished
    previous: /newsletter/2020/05
    next: /fiction/clippy
    importance: 10
    confidence: likely
    cssExtension: drop-caps-kanzlei
    """
    assert GwernBlog._get_metadata(text) == {
        'confidence': 'likely',
        'created': '2020-05-28',
        'cssExtension': 'drop-caps-kanzlei',
        'importance': '10',
        'modified': '2022-01-02',
        'next': '/fiction/clippy',
        'previous': '/newsletter/2020/05',
        'status': 'finished',
        'thumbnail': '/doc/ai/nn/transformer/gpt/2020-brown-gpt3-figure13-meanperformancescalingcurve.png',
        'title': '"The Scaling Hypothesis"',
    }


def test_gwern_process_markdown():
    text = f"""
    ---
    title: "The Scaling Hypothesis"
    created: 2020-05-28
    ...
    {SAMPLE_HTML}
    """
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])

    assert dataset._process_markdown('http://article.url', Mock(text=text)) == {
        'authors': ['Gwern Branwen'],
        'date_published': '2020-05-28T00:00:00Z',
        'id': None,
        'source': 'gwern_blog',
        'source_type': 'blog',
        'summary': [],
        'text': 'bla bla bla [a link](http://ble.com) bla bla',
        'title': '"The Scaling Hypothesis"',
        'url': 'http://article.url',
    }


def test_gwern_process_entry_markdown():
    text = f"""
    ---
    title: "The Scaling Hypothesis"
    created: 2020-05-28
    ...
    {SAMPLE_HTML}
    """
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])

    with patch('requests.get', return_value=Mock(text=text, status_code=200, headers={})):
        assert dataset.process_entry('http://article.url') == {
            'authors': ['Gwern Branwen'],
            'date_published': '2020-05-28T00:00:00Z',
            'id': None,
            'source': 'gwern_blog',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla [a link](http://ble.com) bla bla',
            'title': '"The Scaling Hypothesis"',
            'url': 'http://article.url',
        }


def test_gwern_process_entry_html():
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])

    with patch('requests.get', return_value=Mock(content=GWERN_CONTENTS, status_code=200, headers={'Content-Type': 'text/html'})):
        assert dataset.process_entry('http://article.url') == {
            'authors': ['Gwern Branwen'],
            'date_published': '2023-01-01T00:00:00Z',
            'id': None,
            'source': 'gwern_blog',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla [a link](http://ble.com) bla bla',
            'title': 'The title of the article',
            'url': 'http://article.url',
        }


def test_gwern_process_entry_erro():
    dataset = GwernBlog(name="gwern_blog", url='https://www.gwern.net/', authors=["Gwern Branwen"])

    with patch('requests.get', return_value=Mock(status_code=404)):
        assert dataset.process_entry('http://article.url') is None


MEDIUM_HTML = f"""
    <article>
      <div>
        <h1>This is the title</h1>
        <div>
          <span>Some random thing</span>
          <span>another random thing</span>
          <span>Oct 7, 2023</span>
          <span>more random stuff</span>
         </div>
       </div>
       {SAMPLE_HTML}
    </article>
    """
def test_medium_get_published_date():
    dataset = MediumBlog(name="deepmind_blog", url="https://bla.medium.com/", authors=["mr Blobby"])

    soup = BeautifulSoup(MEDIUM_HTML, "html.parser")
    assert dataset._get_published_date(soup) == '2023-10-07T00:00:00Z'


def test_medium_get_text():
    dataset = MediumBlog(name="deepmind_blog", url="https://bla.medium.com/", authors=["mr Blobby"])

    soup = BeautifulSoup(MEDIUM_HTML, "html.parser")
    soup.find('h1').extract()
    assert dataset._get_text(soup) == 'bla bla bla [a link](http://ble.com) bla bla'


def test_medium_process_entry():
    dataset = MediumBlog(name="deepmind_blog", url="https://bla.medium.com/", authors=["mr Blobby"])

    item = """
    <article>
       <a href="/discovering-when-an-agent-is-present-in-a-system-41154de11e7b"></a>
       <h2>Discovering when an agent is present in a system</h2>
    </article>
    """
    with patch('requests.get', return_value=Mock(content=MEDIUM_HTML)):
        assert dataset.process_entry(BeautifulSoup(item, "html.parser")) == {
            'authors': ['mr Blobby'],
            'date_published': '2023-10-07T00:00:00Z',
            'id': None,
            'source': 'deepmind_blog',
            'source_type': 'blog',
            'summary': [],
            'text': 'bla bla bla [a link](http://ble.com) bla bla',
            'title': 'This is the title',
            'url': 'https://bla.medium.com/discovering-when-an-agent-is-present-in-a-system-41154de11e7b',
        }


def test_substack_blog_process_entry():
    dataset = SubstackBlog(name="blog", url="https://blog.substack.com")
    contents = {
        'entries': [
            {
                'link': 'http://example.org/bla',
                'title': 'the article title',
                'pubDate': 'Mon, 26 Jun 2023 13:40:01 GMT',
                'description': 'the articles description',
                'content': [{'value': SAMPLE_HTML}],
                'authors': [{'name': 'mr Blobby'}],
            }
        ]
    }
    # Setup the items list contents
    with patch('feedparser.parse', return_value=contents):
        dataset.items_list

    assert dataset.process_entry('http://example.org/bla') == {
        'authors': ['mr Blobby'],
        'date_published': '2023-06-26T13:40:01Z',
        'id': None,
        'source': 'blog',
        'source_type': 'blog',
        'summary': [],
        'text': 'bla bla bla [a link](http://ble.com) bla bla',
        'title': 'the article title',
        'url': 'http://example.org/bla',
    }

WORDPRESS_FEED = {
    "entries": [
        {
            "title": "Prospiracy Theory",
            "link": "https://www.yudkowsky.net/other/fiction/prospiracy-theory",
            "authors": [{"name": "Eliezer S. Yudkowsky"}],
            "published": "Fri, 04 Sep 2020 04:11:23 +0000",
            "summary": "Rwanda and I sat on a park bench. Above us the birds flutteredgracefully through a shamefully blue s",
            "content": [{"value": SAMPLE_HTML}],
        },
    ],
    "feed": {
        "title": "Eliezer S. Yudkowsky",
        "link": "https://www.yudkowsky.net",
    },
    "headers": {
        "link": "<https://www.yudkowsky.net/wp-json/>; rel=\"https://api.w.org/\""
    }
}


def test_wordpress_blog_setup():
    blog = WordpressBlog(
        name='blog',
        url="https://www.bla.yudkowsky.net",
    )
    blog.setup()
    assert blog.feed_url == 'https://www.bla.yudkowsky.net/feed'
    assert blog.name == "www.bla.yudkowsky.net"

@patch('feedparser.parse', return_value=WORDPRESS_FEED)
def test_wordpress_blog_items_list(feedparser_parse):
    blog = WordpressBlog(
        name='blog',
        url="https://www.bla.yudkowsky.net",
    )
    blog.setup()
    items = blog.items_list
    assert len(items) == 1
    assert items[0]['title'] == 'Prospiracy Theory'
    

def test_wordpress_blog_get_item_key():
    blog = WordpressBlog(
        name='blog',
        url="https://www.bla.yudkowsky.net",
    )
    item_key = blog.get_item_key({'title': 'Test Entry'})
    assert item_key == 'Test Entry'
    
def test_wordpress_blog_get_published_date():
    blog = WordpressBlog(
        name='blog',
        url="https://www.bla.yudkowsky.net",
    )
    date_published = blog._get_published_date({'published': "Mon, 26 Jun 2023 13:40:01 +0000"})
    assert date_published == '2023-06-26T13:40:01Z'

@patch('feedparser.parse', return_value=WORDPRESS_FEED)
def test_wordpress_blog_fetch_entries(feedparser_parse):
    blog = WordpressBlog(
        name='blog',
        url="https://www.bla.yudkowsky.net",
    )
    blog.setup()
    entries = list(blog.fetch_entries())
    assert len(entries) == 1
    entry = entries[0].to_dict()
    assert entry['url'] == 'https://www.yudkowsky.net/other/fiction/prospiracy-theory'
    assert entry['title'] == 'Prospiracy Theory'
    assert entry['source'] == 'www.bla.yudkowsky.net'
    assert entry['source_type'] == 'blog'
    assert entry['date_published'] == '2020-09-04T04:11:23Z'
    assert entry['authors'] == ['Eliezer S. Yudkowsky']
    assert entry['text'] == 'Prospiracy Theory\n\nbla bla bla [a link](http://ble.com) bla bla'