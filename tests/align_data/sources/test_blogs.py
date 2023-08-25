from unittest.mock import patch, Mock

import pytest
from bs4 import BeautifulSoup
from dateutil.parser import parse

from align_data.sources.blogs import (
    AXRPDataset,
    CaradoMoe,
    ColdTakes,
    GenerativeInk,
    GwernBlog,
    MediumParser,
    SubstackBlog,
    WordpressBlog,
    OpenAIResearch,
    DeepMindTechnicalBlog,
    TransformerCircuits,
)
from align_data.sources.blogs.blogs import EleutherAI


SAMPLE_HTML = """
<div>
  bla bla bla <a href="http://ble.com">a link</a> bla bla
</div>
"""


def test_cold_takes_published_date():
    dataset = ColdTakes(
        name="cold_takes",
        url="https://www.cold-takes.com/",
        authors=["Holden Karnofsky"],
    )

    contents = """
    <article>
        <header>
            <time datetime="2001-02-03"></time>
        </header>
    </article>
    """
    soup = BeautifulSoup(contents, "html.parser")
    assert dataset._get_published_date(soup) == parse("2001-02-03T00:00:00Z")


def test_cold_takes_process_entry():
    dataset = ColdTakes(
        name="cold_takes",
        url="https://www.cold-takes.com/",
        authors=["Holden Karnofsky"],
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

    with patch("requests.get", return_value=Mock(content=article)):
        assert dataset.process_entry(BeautifulSoup(item, "html.parser")).to_dict() == {
            "authors": ["Holden Karnofsky"],
            "date_published": "2023-02-28T00:00:00Z",
            "id": None,
            "source": "cold_takes",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla",
            "title": "What does Bing Chat tell us about AI risk?",
            "url": "https://www.cold-takes.com/how-governments-can-help-with-the-most-important-century/",
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
        authors=["janus"],
    )

    soup = BeautifulSoup(GENERITIVE_INK_HTML, "html.parser")
    assert dataset._get_published_date(soup) == parse("2023-02-09T00:00:00Z")


def test_generative_ink_process_entry():
    dataset = GenerativeInk(
        name="generative.ink",
        url="https://generative.ink/posts/",
        authors=["janus"],
    )

    item = """
    <div class="post on-list">
      <div>
        <span class="post-title"><a href="/posts/simulators/"><strong>Simulators</strong></a></span>
        <span class="post-day" style="float:right">Sep 2</span>
      </div>
    </div>
    """
    with patch("requests.get", return_value=Mock(content=GENERITIVE_INK_HTML)):
        assert dataset.process_entry(BeautifulSoup(item, "html.parser")).to_dict() == {
            "authors": ["janus"],
            "date_published": "2023-02-09T00:00:00Z",
            "id": None,
            "source": "generative.ink",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla",
            "title": "Anomalous tokens reveal the original identities of Instruct models",
            "url": "https://generative.ink/posts/simulators/",
        }


def test_caradomoe_text():
    dataset = CaradoMoe(
        name="carado.moe",
        url="https://carado.moe",
        authors=["Tamsin Leake"],
    )
    contents = f"""
    <div>
        <p class="postmeta"></p>"
        {SAMPLE_HTML}
    </div>
    """
    soup = BeautifulSoup(contents, "html.parser")
    assert dataset._get_text({"soup": soup}) == "bla bla bla [a link](http://ble.com) bla bla"


def test_caradomoe_process_entry():
    dataset = CaradoMoe(
        name="carado.moe",
        url="https://carado.moe",
        authors=["Tamsin Leake"],
    )
    item = {
        "pubDate": "Sat, 10 Jun 2023 07:00:00 -0000",
        "title": "the title",
        "link": "http://example.com/bla",
    }
    dataset.items = {item["link"]: item}
    contents = f"""
    <div>
        <p class="postmeta"></p>"
        {SAMPLE_HTML}
    </div>
    """
    with patch("requests.get", return_value=Mock(content=contents)):
        assert dataset.process_entry(item["link"]).to_dict() == {
            "authors": ["Tamsin Leake"],
            "date_published": "2023-06-10T07:00:00Z",
            "id": None,
            "source": "carado.moe",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla [a link](http://ble.com) bla bla",
            "title": "the title",
            "url": "http://example.com/bla",
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
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])

    soup = BeautifulSoup(GWERN_CONTENTS, "html.parser")
    assert dataset._get_text(soup) == "bla bla bla [a link](http://ble.com) bla bla"


@pytest.mark.parametrize(
    "metadata, date",
    (
        ({"modified": "2022-01-02"}, parse("2022-01-02T00:00:00Z")),
        ({"created": "2022-01-02"}, parse("2022-01-02T00:00:00Z")),
        (
            {"created": "2000-01-01", "modified": "2022-01-02"},
            parse("2022-01-02T00:00:00Z"),
        ),
        ({}, None),
        ({"bla": "asda"}, None),
    ),
)
def test_gwern_get_published_date(metadata, date):
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])

    assert dataset._get_published_date(metadata) == date


def test_gwern_get_article():
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])
    with patch("requests.get", return_value="article contents"):
        assert dataset._get_article("http://bla.com") == "article contents"


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
        "confidence": "likely",
        "created": "2020-05-28",
        "cssExtension": "drop-caps-kanzlei",
        "importance": "10",
        "modified": "2022-01-02",
        "next": "/fiction/clippy",
        "previous": "/newsletter/2020/05",
        "status": "finished",
        "thumbnail": "/doc/ai/nn/transformer/gpt/2020-brown-gpt3-figure13-meanperformancescalingcurve.png",
        "title": '"The Scaling Hypothesis"',
    }


def test_gwern_process_markdown():
    text = f"""
    ---
    title: "The Scaling Hypothesis"
    created: 2020-05-28
    ...
    {SAMPLE_HTML}
    """
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])

    assert dataset._process_markdown("http://article.url", Mock(text=text)).to_dict() == {
        "authors": ["Gwern Branwen"],
        "date_published": "2020-05-28T00:00:00Z",
        "id": None,
        "source": "gwern_blog",
        "source_type": "blog",
        "summaries": [],
        "text": "bla bla bla [a link](http://ble.com) bla bla",
        "title": '"The Scaling Hypothesis"',
        "url": "http://article.url",
    }


def test_gwern_process_entry_markdown():
    text = f"""
    ---
    title: "The Scaling Hypothesis"
    created: 2020-05-28
    ...
    {SAMPLE_HTML}
    """
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])

    with patch("requests.get", return_value=Mock(text=text, status_code=200, headers={})):
        assert dataset.process_entry("http://article.url").to_dict() == {
            "authors": ["Gwern Branwen"],
            "date_published": "2020-05-28T00:00:00Z",
            "id": None,
            "source": "gwern_blog",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla [a link](http://ble.com) bla bla",
            "title": '"The Scaling Hypothesis"',
            "url": "http://article.url",
        }


def test_gwern_process_entry_html():
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])

    with patch(
        "requests.get",
        return_value=Mock(
            content=GWERN_CONTENTS,
            status_code=200,
            headers={"Content-Type": "text/html"},
        ),
    ):
        assert dataset.process_entry("http://article.url").to_dict() == {
            "authors": ["Gwern Branwen"],
            "date_published": "2023-01-01T00:00:00Z",
            "id": None,
            "source": "gwern_blog",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla [a link](http://ble.com) bla bla",
            "title": "The title of the article",
            "url": "http://article.url",
        }


def test_gwern_process_entry_erro():
    dataset = GwernBlog(name="gwern_blog", url="https://www.gwern.net/", authors=["Gwern Branwen"])

    with patch("requests.get", return_value=Mock(status_code=404)):
        assert dataset.process_entry("http://article.url") is None


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
    dataset = MediumParser(
        name="deepmind_blog", url="https://bla.medium.com/", authors=["mr Blobby"]
    )

    soup = BeautifulSoup(MEDIUM_HTML, "html.parser")
    assert dataset._get_published_date(soup) == parse("2023-10-07T00:00:00Z")


def test_medium_get_text():
    dataset = MediumParser(
        name="deepmind_blog", url="https://bla.medium.com/", authors=["mr Blobby"]
    )

    soup = BeautifulSoup(MEDIUM_HTML, "html.parser")
    soup.find("h1").extract()
    assert dataset._get_text(soup) == "bla bla bla [a link](http://ble.com) bla bla"


def test_medium_process_entry():
    dataset = MediumParser(
        name="deepmind_blog", url="https://bla.medium.com/", authors=["mr Blobby"]
    )

    item = """
    <article>
       <a href="/discovering-when-an-agent-is-present-in-a-system-41154de11e7b"></a>
       <h2>Discovering when an agent is present in a system</h2>
    </article>
    """
    with patch("requests.get", return_value=Mock(content=MEDIUM_HTML)):
        assert dataset.process_entry(BeautifulSoup(item, "html.parser")).to_dict() == {
            "authors": ["mr Blobby"],
            "date_published": "2023-10-07T00:00:00Z",
            "id": None,
            "source": "deepmind_blog",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla [a link](http://ble.com) bla bla",
            "title": "This is the title",
            "url": "https://bla.medium.com/discovering-when-an-agent-is-present-in-a-system-41154de11e7b",
        }


def test_substack_blog_process_entry():
    dataset = SubstackBlog(name="blog", url="https://blog.substack.com")
    contents = {
        "entries": [
            {
                "link": "http://example.org/bla",
                "title": "the article title",
                "pubDate": "Mon, 26 Jun 2023 13:40:01 GMT",
                "description": "the articles description",
                "content": [{"value": SAMPLE_HTML}],
                "authors": [{"name": "mr Blobby"}],
            }
        ]
    }
    # Setup the items list contents
    with patch("feedparser.parse", return_value=contents):
        dataset.items_list

    assert dataset.process_entry("http://example.org/bla").to_dict() == {
        "authors": ["mr Blobby"],
        "date_published": "2023-06-26T13:40:01Z",
        "id": None,
        "source": "blog",
        "source_type": "blog",
        "summaries": [],
        "text": "bla bla bla [a link](http://ble.com) bla bla",
        "title": "the article title",
        "url": "http://example.org/bla",
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
    "headers": {"link": '<https://www.yudkowsky.net/wp-json/>; rel="https://api.w.org/"'},
}


def test_wordpress_blog_setup():
    blog = WordpressBlog(
        name="blog_name",
        url="https://www.bla.yudkowsky.net",
    )
    assert blog.feed_url == "https://www.bla.yudkowsky.net/feed"
    assert blog.name == "blog_name"


@patch("feedparser.parse", return_value=WORDPRESS_FEED)
def test_wordpress_blog_items_list(feedparser_parse):
    blog = WordpressBlog(name="blog", url="https://www.bla.yudkowsky.net")
    assert blog.items_list == ["https://www.yudkowsky.net/other/fiction/prospiracy-theory"]


def test_wordpress_blog_get_item_key():
    blog = WordpressBlog(
        name="blog",
        url="https://www.bla.yudkowsky.net",
    )
    item = {"title": "Test Entry"}
    assert item == blog.get_item_key(item)


def test_wordpress_blog_get_published_date():
    blog = WordpressBlog(
        name="blog",
        url="https://www.bla.yudkowsky.net",
    )
    date_published = blog._get_published_date({"published": "Mon, 26 Jun 2023 13:40:01 +0000"})
    assert date_published == parse("2023-06-26T13:40:01Z")


@patch("feedparser.parse", return_value=WORDPRESS_FEED)
def test_wordpress_blog_process_entry(feedparser_parse):
    blog = WordpressBlog(
        name="blog_name",
        url="https://www.bla.yudkowsky.net",
    )
    blog.items = {i["link"]: i for i in WORDPRESS_FEED["entries"]}
    entry = blog.process_entry("https://www.yudkowsky.net/other/fiction/prospiracy-theory")
    assert entry.to_dict() == {
        "authors": ["Eliezer S. Yudkowsky"],
        "date_published": "2020-09-04T04:11:23Z",
        "id": None,
        "source": "blog_name",
        "source_type": "blog",
        "summaries": [],
        "text": "bla bla bla [a link](http://ble.com) bla bla",
        "title": "Prospiracy Theory",
        "url": "https://www.yudkowsky.net/other/fiction/prospiracy-theory",
    }


ELEUTHER_HTML = """
    <article class="post-single">
      <header class="post-header">
        <h1 class="post-title">Minetester: A fully open RL environment built on Minetest</h1>
        <div class="post-description">An overview of the minetester and preliminary work</div>
        <div class="post-meta">July 8, 2023&nbsp;·&nbsp;Curtis Huebner, Robert Klassert, Stepan Shabalin, Edwin Fennell, Delta Hessler
</div>
      </header>
      <div class="post-content">
        bla bla bla
      </div>
    </article>
"""


def test_eleutherai_get_published_date():
    dataset = EleutherAI(name="eleuther", url="http://bla.bla")

    soup = BeautifulSoup(ELEUTHER_HTML, "html.parser")
    assert dataset._get_published_date(soup) == parse("2023-07-08T00:00:00Z")


def test_eleutherai_extract_authors():
    dataset = EleutherAI(name="eleuther", url="http://bla.bla")

    soup = BeautifulSoup(ELEUTHER_HTML, "html.parser")
    assert dataset.extract_authors(soup) == [
        "Curtis Huebner",
        "Robert Klassert",
        "Stepan Shabalin",
        "Edwin Fennell",
        "Delta Hessler",
    ]


def test_eleutherai_process_entry():
    dataset = EleutherAI(name="eleuther", url="http://bla.bla")

    article = BeautifulSoup('<a href="bla.bla"></a>', "html.parser")
    with patch("requests.get", return_value=Mock(content=ELEUTHER_HTML)):
        assert dataset.process_entry(article).to_dict() == {
            "authors": [
                "Curtis Huebner",
                "Robert Klassert",
                "Stepan Shabalin",
                "Edwin Fennell",
                "Delta Hessler",
            ],
            "date_published": "2023-07-08T00:00:00Z",
            "id": None,
            "source": "eleuther",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla",
            "title": "Minetester: A fully open RL environment built on Minetest",
            "url": "http://bla.bla/bla.bla",
        }


OPENAI_HTML = """
<div class="container">
  <div class="cols-container">
    <span class="f-meta-2">July 6, 2023</span>
    <a class="ui-link" href="https://arxiv.org">Read paper</a>
  </div>
  <div>
    <div>Authors</div>
    <div>
      <div class="f-body-1"><p>Mr. Blobby<br>John Snow (Westeros)</p>
    </div>
  </div>
</div>
"""


def test_openai_research_get_published_date():
    dataset = OpenAIResearch(name="openai", url="bla.bla")

    soup = BeautifulSoup(OPENAI_HTML, "html.parser")
    assert dataset._get_published_date(soup) == parse("2023-07-06T00:00:00Z")


def test_openai_research_get_text():
    dataset = OpenAIResearch(name="openai", url="bla.bla")

    soup = BeautifulSoup(OPENAI_HTML, "html.parser")
    parsers = {"arxiv.org": lambda _: {"text": "bla bla bla"}}
    with patch("requests.head", return_value=Mock(headers={"Content-Type": "text/html"})):
        with patch("align_data.sources.articles.parsers.PDF_PARSERS", parsers):
            assert dataset._get_text(soup) == "bla bla bla"


@pytest.mark.parametrize(
    "html, expected",
    (
        (
            """<div>
          <div>Authors</div>
          <div>
             <div class="f-body-1"><p>Mr. Blobby<br>John Snow (Westeros)</p>
          </div>
        </div>
        """,
            ["Mr. Blobby", "John Snow"],
        ),
        (
            """<div>
          <div>Acknowledgments</div>
          <div>
             <div class="f-body-1"><p>Mr. Blobby<br>John Snow (Westeros)</p>
          </div>
        </div>
        """,
            ["Mr. Blobby", "John Snow"],
        ),
        (
            """<div>
          <div>Bla Bla Bla</div>
          <div>
             <div class="f-body-1"><p>Mr. Blobby<br>John Snow (Westeros)</p>
          </div>
        </div>
        """,
            ["OpenAI Research"],
        ),
    ),
)
def test_openai_research_extract_authors(html, expected):
    dataset = OpenAIResearch(name="openai", url="bla.bla")

    soup = BeautifulSoup(html, "html.parser")
    assert dataset.extract_authors(soup) == expected


def test_openai_research_process_entry():
    dataset = OpenAIResearch(name="openai", url="bla.bla")

    soup = BeautifulSoup(OPENAI_HTML, "html.parser")
    parsers = {"arxiv.org": lambda _: {"text": "bla bla bla"}}
    with patch("requests.head", return_value=Mock(headers={"Content-Type": "text/html"})):
        with patch("requests.get", return_value=Mock(content=OPENAI_HTML)):
            with patch("align_data.sources.articles.parsers.PDF_PARSERS", parsers):
                assert dataset.process_entry(soup).to_dict() == {
                    "authors": ["Mr. Blobby", "John Snow"],
                    "date_published": "2023-07-06T00:00:00Z",
                    "id": None,
                    "source": "openai",
                    "source_type": "blog",
                    "summaries": [],
                    "text": "bla bla bla",
                    "title": None,
                    "url": "https://arxiv.org",
                }


def test_deepmind_technical_items_list():
    dataset = DeepMindTechnicalBlog(name="bla", url="http://bla.com")

    def getter(url, *args, **params):
        page = params.get("params")["73df3071_page"]
        if page < 3:
            html = "".join(
                f'<div class="w-dyn-item"><div class="c_card_list__item__blog">{i}</div></div>'
                for i in range(page * 10 - 10, page * 10)
            )
            return Mock(content=f"<div>{html}</div>")
        return Mock(content="")

    with patch("requests.get", getter):
        assert [str(i) for i in dataset.items_list] == [
            f'<div class="c_card_list__item__blog">{i}</div>' for i in range(0, 20)
        ]


DEEPMIND_HTML = """
<div>
  <div class="c_banner__blog__card">
    <h2>title!</h2>
    <div class="c_banner__blog__card__meta">July 11, 2023</div>
  </div>
  <div class="c_rich-text__cms">
    bla bla bla
    <div class="article-gtag-buttons">
       this should be ignored
    </div>
   </div>
   <div class="c_cms_content__meta__wrapper">
     <div>Authors</div>
     <div>Mr. Blobby, John Snow</div>
  </div>
</div>
"""


def test_deepmind_technical_get_published_date():
    dataset = DeepMindTechnicalBlog(name="bla", url="http://bla.com")
    soup = BeautifulSoup(DEEPMIND_HTML, "html.parser")
    assert dataset._get_published_date(soup) == parse("2023-07-11T00:00:00Z")


def test_deepmind_technical_extract_authors():
    dataset = DeepMindTechnicalBlog(name="bla", url="http://bla.com")
    soup = BeautifulSoup(DEEPMIND_HTML, "html.parser")
    assert dataset.extract_authors(soup) == ["Mr. Blobby", "John Snow"]


def test_deepmind_technical_proces_entry():
    dataset = DeepMindTechnicalBlog(name="bla", url="http://bla.com")
    soup = BeautifulSoup('<div><a href="http://bla.bl"></a></div>', "html.parser")
    with patch("requests.get", return_value=Mock(content=DEEPMIND_HTML)):
        assert dataset.process_entry(soup).to_dict() == {
            "authors": ["Mr. Blobby", "John Snow"],
            "date_published": "2023-07-11T00:00:00Z",
            "id": None,
            "source": "bla",
            "source_type": "blog",
            "summaries": [],
            "text": "bla bla bla",
            "title": "title!",
            "url": "http://bla.bl",
        }


TRANSFORMER_CIRCUITS_HTML = """<html>
  <head>
    <title>This is the title</title>
  </head>
  <body>
     <d-title>
            <h1>This is also the title</h1>
      </d-title>
      <div class="d-byline-container base-grid">
         <div class="d-byline">
            <div class="authors" style="grid-area: authors">
               <h3>Authors</h3>
               <div>
                 <span class="author"><a href="https://nelhage.com/">Nelson Elhage<sup>∗</sup></a>,</span>
                 <span class="author">Robert Lasenby,</span>
                 <span class="author"><a href="https://colah.github.io/">Christopher Olah<sup>‡</sup></a></span>
               </div>
             </div>
             <div class="affiliations" style="grid-area: affiliations">
                <h3>Affiliation</h3>
                <div><a href="https://www.anthropic.com/">Anthropic</a></div>
             </div>
             <div class="published" style="grid-area: published">
                <h3>Published</h3>
                <div>March 16, 2023</div>
             </div>
           </div>
         </div>
      </div>
      <d-article>
         This is where the text goes. With a <a href="bla.com">link</a> to test
      </d-article>
  </body>
</html>
"""

def test_transformer_circuits_item_key():
    dataset = TransformerCircuits(url='http://bla.com', name='ble')
    html = """<div>
    <a class="paper" href="2023/july-update/index.html">
        <h3>Circuits Updates — July 2023</h3>
        <div class="byline"></div>
        <div class="description">
        A collection of small updates from the Anthropic Interpretability Team.
        </div>
    </a></div>"""
    assert dataset.get_item_key(BeautifulSoup(html, 'html.parser').find('a')) == 'http://bla.com/2023/july-update/index.html'


def test_transformer_circuits_item_list():
    dataset = TransformerCircuits(url='http://bla.com', name='ble')
    html = """<div>
    <div class="toc">
        <a href="item1.html"></a>
        <a href="item2.html"></a>
        <a href="item3.html"></a>
        <a href="http://bla.com/item4.html"></a>

        <a href="http://this.will.be.skipped"></a>
    </div></div>"""
    with patch("requests.get", return_value=Mock(content=html)):
        assert [i.get('href') for i in dataset.items_list] == [
            'item1.html', 'item2.html', 'item3.html', 'http://bla.com/item4.html'
        ]


def test_transformer_circuits_get_title():
    dataset = TransformerCircuits(url='http://bla.com', name='ble')
    soup = BeautifulSoup(TRANSFORMER_CIRCUITS_HTML, "html.parser")
    assert dataset._get_title(soup) == "This is the title"


def test_transformer_circuits_get_published_date():
    dataset = TransformerCircuits(url='http://bla.com', name='ble')
    soup = BeautifulSoup(TRANSFORMER_CIRCUITS_HTML, "html.parser")
    assert dataset._get_published_date(soup).isoformat() == "2023-03-16T00:00:00+00:00"


def test_transformer_circuits_get_text():
    dataset = TransformerCircuits(url='http://bla.com', name='ble')
    soup = BeautifulSoup(TRANSFORMER_CIRCUITS_HTML, "html.parser")
    assert dataset._get_text(soup) == "This is where the text goes. With a [link](bla.com) to test"


def test_transformer_circuits_process_item():
    dataset = TransformerCircuits(url='http://bla.com', name='ble')
    item = BeautifulSoup('<a href="ble/bla"</a>', "html.parser").find('a')
    with patch("requests.get", return_value=Mock(content=TRANSFORMER_CIRCUITS_HTML)):
        assert dataset.process_entry(item).to_dict() == {
            'authors': ['Nelson Elhage', 'Robert Lasenby', 'Christopher Olah'],
            'date_published': '2023-03-16T00:00:00Z',
            'id': None,
            'source': 'ble',
            'source_type': 'blog',
            'summaries': [],
            'text': 'This is where the text goes. With a [link](bla.com) to test',
            'title': 'This is the title',
            'url': 'http://bla.com/ble/bla',
        }


@pytest.mark.parametrize('url, expected', (
    ('/a/path', 'https://ble.ble.com/a/path'),
    ('http://ble.ble.com/bla', 'http://ble.ble.com/bla'),
    ('https://ble.ble.com/bla', 'https://ble.ble.com/bla'),
))
def test_axrp_dataset_extract_item_url(url, expected):
    dataset = AXRPDataset(name='bla', url='https://ble.ble.com')
    assert dataset._extract_item_url({'link': url}) == expected


@pytest.mark.parametrize('item, expected', (
    ({}, ['default authors']),
    ({'authors': []}, ['default authors']),
    ({'authors': [{'bla': 'bla'}]}, ['default authors']),
    ({'authors': [{'name': ''}]}, ['default authors']),
    ({'authors': [{'name': '    \t    \n'}]}, ['default authors']),

    ({'title': 'bla bla bla'}, ['default authors']),
    ({'title': 'bla bla bla with'}, ['default authors']),
    ({'title': 'bla bla bla with     \t    \n'}, ['default authors']),

    ({'authors': [{'name': 'mr. blobby'}]}, ['mr. blobby']),
    ({'authors': [{'name': 'mr. blobby'}, {'name': 'janek'}]}, ['mr. blobby', 'janek']),

    ({'title': 'bla bla bla with your momma'}, ['default authors', 'your momma']),
))
def test_axrp_dataset_extract_authors(item, expected):
    dataset = AXRPDataset(name='bla', url='https://ble.ble.com', authors=['default authors'])
    assert dataset.extract_authors(item) == expected


def test_axrp_dataset_process_entry():
    dataset = AXRPDataset(name='bla', url='https://ble.ble.com', authors=['default authors'])
    url = 'https://ble.ble.com/ble/ble'
    dataset.items = {
        url: {
            'content': [{'value': 'bla bla'}],
            'link': '/ble/ble',
            'published': '2023-07-27T03:50:00+00:00',
            'title': 'Something or other with your momma',
        }
    }
    assert dataset.process_entry(url).to_dict() == {
        'authors': ['default authors', 'your momma'],
        'date_published': '2023-07-27T03:50:00Z',
        'id': None,
        'source': 'bla',
        'source_type': 'blog',
        'summaries': [],
        'text': 'bla bla',
        'title': 'Something or other with your momma',
        'url': 'https://ble.ble.com/ble/ble',
    }
