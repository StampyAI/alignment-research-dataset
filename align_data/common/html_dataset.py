import pytz
import regex as re
import logging
from datetime import datetime
from dataclasses import dataclass, field, KW_ONLY
from urllib.parse import urljoin
from typing import List

import requests
import feedparser
from bs4 import BeautifulSoup
from markdownify import markdownify

from align_data.common.alignment_dataset import AlignmentDataset

logger = logging.getLogger(__name__)


@dataclass
class HTMLDataset(AlignmentDataset):
    """
    Fetches articles from a different blog by collecting links to articles from an index page.
    """

    url: str
    done_key = "url"

    authors: List[str] = field(default_factory=list)
    _: KW_ONLY
    source_key: str = None
    summary_key: str = None

    item_selector = "article"
    title_selector = "article h1"
    text_selector = "article"
    source_type = "blog"
    ignored_selectors = []

    def extract_authors(self, article):
        return self.authors

    def get_item_key(self, item):
        article_url = item.find_all("a")[0]["href"].split("?")[0]
        return urljoin(self.url, article_url)

    @property
    def items_list(self):
        logger.info(f"Fetching entries from {self.url}")
        response = requests.get(self.url, allow_redirects=True)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = soup.select(self.item_selector)
        logger.info(f"Found {len(articles)} articles")
        return articles

    def _extra_values(self, contents):
        return {}

    def get_contents(self, article_url):
        contents = self.fetch_contents(article_url)

        title = self._get_title(contents)
        date_published = self._get_published_date(contents)

        return {
            "text": self._get_text(contents),
            "url": article_url,
            "title": title,
            "source": self.name,
            "source_type": "blog",
            "date_published": date_published,
            "authors": self.extract_authors(contents),
            **self._extra_values(contents),
        }

    def process_entry(self, article):
        article_url = self.get_item_key(article)
        contents = self.get_contents(article_url)
        if not contents.get('text'):
            return None

        return self.make_data_entry(contents)

    def fetch_contents(self, url):
        logger.info("Fetching {}".format(url))
        resp = requests.get(url, allow_redirects=True)
        return BeautifulSoup(resp.content, "html.parser")

    def _get_title(self, contents):
        title = contents.select_one(self.title_selector)
        return title and title.extract().text.strip()

    def _get_text(self, contents):
        article = contents.select_one(self.text_selector)
        if not article:
            return None

        for selector in self.ignored_selectors:
            for elem in article.select(selector):
                elem.extract()
        return self._extract_markdown(article)

    def _find_date(self, items):
        for i in items:
            if re.match("\w+ \d{1,2}, \d{4}", i.text):
                return datetime.strptime(i.text, "%b %d, %Y").replace(tzinfo=pytz.UTC)

    def _extract_markdown(self, element):
        return element and markdownify(str(element)).strip()


@dataclass
class RSSDataset(HTMLDataset):
    date_format = "%a, %d %b %Y %H:%M:%S %z"

    def get_item_key(self, item):
        return item

    @property
    def feed_url(self):
        return f"{self.url}/rss.xml"

    def extract_authors(self, item):
        if "authors" in item:
            return [a["name"] for a in item["authors"] if a.get("name")]
        return self.authors

    @staticmethod
    def _get_title(item):
        return item["title"]

    def _get_published_date(self, item):
        date_published = item.get("published") or item.get("pubDate")
        return super()._get_published_date(date_published)

    def _get_text(self, item):
        text = item.get("content") and item["content"][0].get("value")
        return self._extract_markdown(text)

    def fetch_contents(self, url):
        item = self.items[url]
        if "content" in item:
            return item

        logger.info("Fetching {}".format(url))
        resp = requests.get(url, allow_redirects=True)
        soup = BeautifulSoup(resp.content, "html.parser")
        return dict(
            self.items[url],
            soup=soup,
        )

    @property
    def items_list(self):
        logger.info(f"Fetching entries from {self.feed_url}")
        feed = feedparser.parse(self.feed_url)
        self.items = {item["link"]: item for item in feed["entries"]}
        return list(self.items.keys())
