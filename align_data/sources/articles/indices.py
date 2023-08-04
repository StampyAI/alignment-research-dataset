import logging
import re
from collections import defaultdict

from dateutil.parser import ParserError, parse
from markdownify import MarkdownConverter
from tqdm import tqdm

from align_data.sources.articles.html import fetch, fetch_element
from align_data.sources.articles.parsers import item_metadata
from align_data.common.alignment_dataset import AlignmentDataset

logger = logging.getLogger(__name__)


def get_text(tag, selector: str) -> str:
    if item := tag.select_one(selector):
        return item.text
    return ""


def indice_fetcher(url, main_selector, item_selector, formatter):
    def fetcher():
        if contents := fetch_element(url, main_selector):
            return list(filter(None, map(formatter, contents.select(item_selector))))
        return []

    return fetcher


def reading_what_we_can_items():
    res = fetch("https://readingwhatwecan.com/books.js")
    items = {
        item
        for section in re.findall(r"\[(.*?)\]", res.text, re.DOTALL)
        for item in re.findall(
            r'Name: "(.*?)",.*?Link: "(.*?)",.*?Author: "(.*?)"', section, re.DOTALL
        )
    }
    return [{"title": item[0], "url": item[1], "authors": item[2]} for item in items]


def aisafetysupport():
    contents = fetch_element(
        "https://www.aisafetysupport.org/resources/lots-of-links", "header + div"
    )
    sections = [
        "Research Maps and Reviews",
        "Research Agendas",
        "Books, papers, podcasts, videos",
    ]
    sections = [s for s in contents.select("section") if get_text(s, "h2") in sections]
    return [
        {"title": a.text, "url": a.get("href")}
        for section in sections
        for a in section.select("a")
        if a.text and a.get("href").startswith("http")
    ]


def format_mlsafety_course(a):
    if (a.get("href") or "").startswith("http"):
        return {"title": a.text, "url": a.get("href")}


def format_anthropic(post):
    if date_published := parse(get_text(post, "div.post-date")):
        date_published = AlignmentDataset._format_datetime(date_published)
    url = post.get("href")

    if source_url := fetch_element(url, "article .post-heading a.btn-primary"):
        source_url = source_url.get("href")

    return {
        "title": get_text(post, "div.post-heading"),
        "url": url,
        "source_url": source_url,
        "date_published": date_published,
    }


def format_transformer_circuits(item):
    if not item.get("href").startswith("http"):
        url = f'https://transformer-circuits.pub/{item.get("href")}'
        return {
            "title": get_text(item, "h3"),
            "url": url,
            "source_url": url,
        }


def format_safe_ai(item):
    return {
        "title": get_text(item, "h4"),
        "url": item.find("a").get("href"),
        "source_url": item.find("a").get("href"),
        "authors": get_text(item, "h4 ~ p"),
    }


def format_far_ai(item):
    return {
        "title": get_text(item, ".article-title"),
        "url": f'https://www.safe.ai/research{item.select_one(".article-title a").get("href")}',
        "source_url": item.select_one('div.btn-links a:-soup-contains("PDF")').get("href"),
        "authors": ", ".join(i.text for i in item.select(".article-metadata a")),
    }


def format_redwoodresearch(item):
    url = item.select_one(".list-item-content__button-container a").get("href")
    authors = get_text(item, "em")
    try:
        parts = authors.split(", ")
        date_published = parse(parts[-1])
        date_published = AlignmentDataset._format_datetime(date_published)
        authors = ", ".join(parts[:-1])
    except ParserError:
        date_published = None

    return {
        "title": get_text(item, "h2"),
        "url": url,
        "source_url": url,
        "authors": authors,
        "date_published": date_published,
    }


def format_chai_research(item):
    author_block = next(item.children).strip().strip(".")
    authors = parts = author_block.split(".")
    try:
        int(parts[-1].strip())
        date_published = parts[-1].strip()
        authors = parts[:-1]
    except ValueError:
        date_published = None

    url = item.select_one("a").get("href")
    return {
        "title": get_text(item, "a"),
        "url": url,
        "source_url": url,
        "authors": ", ".join(authors),
        "date_published": date_published,
    }


def format_chai_bibliography(item):
    return {
        "title": get_text(item, ".bib-entry-title a"),
        "url": item.select_one(".bib-entry-title a").get("href"),
        "authors": item.select_one(".bib-entry-title a").next_sibling.strip(",. "),
    }


def format_chai_newsletter(item):
    if item.text.strip().startswith("CHAI Newsletter"):
        return {
            "title": item.text,
            "url": item.get("href"),
            "source_url": item.get("href"),
        }


def format_neel_nanda_fav(item):
    url = item.find("a").get("href").strip()
    if not url.startswith("http"):
        return None

    try:
        title = item.find("p").extract().text
    except:
        title = get_text(item, "a")

    return {
        "title": title.replace("\n", " "),
        "url": url,
        "summary": MarkdownConverter().convert_soup(item).strip(),
    }


def fetch_all():
    fetchers = [
        reading_what_we_can_items,
        aisafetysupport,
        indice_fetcher(
            "https://www.neelnanda.io/mechanistic-interpretability/favourite-papers",
            "article",
            "div > ul > li",
            format_neel_nanda_fav,
        ),
        indice_fetcher(
            "https://course.mlsafety.org/readings/",
            "div.main-content",
            "a",
            format_mlsafety_course,
        ),
        indice_fetcher(
            "https://www.anthropic.com/research",
            "div.b-postList",
            "a",
            format_anthropic,
        ),
        indice_fetcher(
            "https://transformer-circuits.pub/",
            "div.toc",
            "a",
            format_transformer_circuits,
        ),
        indice_fetcher(
            "https://www.safe.ai/research",
            "#guiding-principles",
            "div.card.is-document",
            format_safe_ai,
        ),
        indice_fetcher(
            "https://far.ai/publication/",
            "#container-publications",
            "div.media-body",
            format_far_ai,
        ),
        indice_fetcher(
            "https://www.redwoodresearch.org/research",
            "article",
            ".list-item",
            format_redwoodresearch,
        ),
        indice_fetcher(
            "https://humancompatible.ai/research",
            "article",
            ".publications li",
            format_chai_research,
        ),
        indice_fetcher(
            "https://humancompatible.ai/bibliography",
            "#content",
            ".bib-entry",
            format_chai_bibliography,
        ),
        indice_fetcher(
            "https://humancompatible.ai/newsletter/",
            "article",
            "a",
            format_chai_newsletter,
        ),
    ]

    articles = defaultdict(dict)
    for func in tqdm(fetchers):
        for item in func():
            articles[item["title"]].update(item)
    return articles


class IndicesDataset(AlignmentDataset):

    done_key = 'url'

    @property
    def items_list(self):
        return fetch_all().values()

    def get_item_key(self, item):
        return item.get('url')

    @staticmethod
    def extract_authors(item):
        if authors := (item.get('authors') or '').strip():
            return [author.strip() for author in authors.split(',') if author.strip()]
        return []

    def process_entry(self, item):
        metadata = {}
        if url := item.get('source_url') or item.get('url'):
            metadata = item_metadata(url)

        text = metadata.get('text')
        if not text:
            logger.error('Could not get text for %s (%s) - %s - skipping for now', item.get('title'), url, metadata.get('error'))
            return None

        return self.make_data_entry({
            'source': self.name,
            'url': self.get_item_key(item),
            'title': item.get('title'),
            'date_published': self._get_published_date(item.get('date_published')),
            'authors': self.extract_authors(item),
            'text': text,
        })
