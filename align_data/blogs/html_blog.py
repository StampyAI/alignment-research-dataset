import regex as re
import time
import logging
from datetime import datetime
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
import feedparser
from bs4 import BeautifulSoup

from align_data.common import utils
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

logger = logging.getLogger(__name__)


@dataclass
class HTMLBlog(AlignmentDataset):
    """
    Fetches articles from a different blog by collecting links to articles from an index page.
    """
    url: str
    done_key = "url"

    title_selector = 'h2'
    item_selector = ['article']
    source_type = "blog"

    cleaner = utils.HtmlCleaner(
        ["You might also like\.\.\..*", "\\n+", "\#\# Create your profile.*"],
        ["", "\\n", ""],
        True,
    )

    def extract_title(self, article):
        title = article.find(self.title_selector)
        if title is None:
            return None
        return title.text

    def get_item_key(self, item):
        article_url = item.find_all("a")[0]["href"].split("?")[0]
        return urljoin(self.url, article_url)

    @property
    def items_list(self):
        logger.info(f"Fetching entries from {self.url}")
        response = requests.get(self.url, allow_redirects=True)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = soup.find_all(*self.item_selector)
        logger.info(f"Found {len(articles)} articles")
        return articles

    def process_entry(self, article):
        article_url = self.get_item_key(article)
        contents = self._get_contents(article_url)

        title = self._get_title(contents)
        date_published = self._get_published_date(contents)
        text = self._get_text(contents)

        return DataEntry({
            "text": text,
            "url": article_url,
            "title": title,
            "source": self.name,
            "source_type": "blog",
            "date_published": date_published,
        })

    def _get_contents(self, url):
        logger.info("Fetching {}".format(url))
        resp = requests.get(url, allow_redirects=True)
        return BeautifulSoup(resp.content, "html.parser")

    @staticmethod
    def _get_title(contents):
        return contents.find('article').find('h1').text

    def _get_text(self, contents):
        return self.cleaner.clean(contents.find('article').text)

    @staticmethod
    def _get_published_date(contents):
        return 'n/a'

    @staticmethod
    def _find_date(items):
        for i in items:
            if re.match('\w+ \d{1,2}, \d{4}', i.text):
                return datetime.strptime(i.text, '%b %d, %Y').date().isoformat()


@dataclass
class RSSBlog(HTMLBlog):

    def get_item_key(self, item):
        return item

    @property
    def items_list(self):
        logger.info(f"Fetching entries from {self.url}")
        feed = feedparser.parse(self.url)
        return [item['link'] for item in feed['entries']]
