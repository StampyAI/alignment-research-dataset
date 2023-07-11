import regex as re
import logging
from datetime import datetime
from dateutil.parser import parse
from dataclasses import dataclass, field, KW_ONLY
from urllib.parse import urljoin
from typing import List

import requests
import feedparser
from bs4 import BeautifulSoup
from markdownify import markdownify

from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

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

    title_selector = 'h2'
    item_selector = 'article'
    source_type = "blog"
    ignored_selectors = []

    def extract_authors(self, article):
        return self.authors

    def extract_title(self, article):
        title = article.find(self.title_selector)
        if title is None:
            return None
        return title.text.strip()

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

    def process_entry(self, article):
        article_url = self.get_item_key(article)
        contents = self._get_contents(article_url)

        title = self._get_title(contents)
        date_published = self._get_published_date(contents)
        text = self._get_text(contents)
        if not text:
            return None

        return DataEntry({
            "text": text,
            "url": article_url,
            "title": title,
            "source": self.name,
            "source_type": "blog",
            "date_published": date_published,
            "authors": self.extract_authors(contents),
            **self._extra_values(contents),
        })

    def _get_contents(self, url):
        logger.info("Fetching {}".format(url))
        resp = requests.get(url, allow_redirects=True)
        return BeautifulSoup(resp.content, "html.parser")

    @staticmethod
    def _get_title(contents):
        title = contents.select_one('article h1')
        return title and title.extract().text.strip()

    def _get_text(self, contents):
        article = contents.find('article')
        for selector in self.ignored_selectors:
            for elem in article.select(selector):
                elem.extract()
        return self._extract_markdown(article)

    @staticmethod
    def _get_published_date(contents):
        return ''

    def _find_date(self, items):
        for i in items:
            if re.match('\w+ \d{1,2}, \d{4}', i.text):
                return self._format_datetime(datetime.strptime(i.text, '%b %d, %Y'))

    def _extract_markdown(self, element):
        return element and markdownify(str(element)).strip()

@dataclass
class RSSDataset(HTMLDataset):
    date_format = '%a, %d %b %Y %H:%M:%S %z'

    def get_item_key(self, item):
        return item

    @property
    def feed_url(self):
        return f'{self.url}/rss.xml'

    def extract_authors(self, item):
        if 'authors' in item:
            return [a['name'] for a in item['authors'] if a.get('name')]
        return self.authors

    @staticmethod
    def _get_title(item):
        return item['title']

    def _get_published_date(self, item):
        date_published = item.get('published') or item.get('pubDate')
        if date_published:
            return self._format_datetime(parse(date_published))
        return ''

    def _get_text(self, item):
        text = item.get('content') and item['content'][0].get('value')
        return self._extract_markdown(text)

    def _get_contents(self, url):
        item = self.items[url]
        if 'content' in item:
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
        self.items = {item['link']: item for item in feed['entries']}
        return list(self.items.keys())
