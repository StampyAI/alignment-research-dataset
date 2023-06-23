import requests
import logging
from dataclasses import dataclass
from datetime import datetime

from align_data.common.alignment_dataset import DataEntry
from align_data.common.html_dataset import HTMLDataset

logger = logging.getLogger(__name__)


@dataclass
class GwernBlog(HTMLDataset):
    """
    Fetches articles from a different blog by collecting links to articles from an index page.
    """

    COOLDOWN: int = 1
    done_key = "url"

    def get_item_key(self, item):
        return item

    @property
    def items_list(self):
        return [
            'https://www.gwern.net/Scaling-hypothesis.page',
            'https://www.gwern.net/Tanks.page',
            'https://www.gwern.net/Clippy.page',
            'https://www.gwern.net/complexity.page',
            'https://www.gwern.net/Tool-AI.page',
            'https://www.gwern.net/Backstop.page',
            'https://www.gwern.net/Hyperbolic-Time-Chamber.page'
        ]

    def process_entry(self, post_href):
        article = self._get_article(post_href)
        if article.status_code != 200:
            logger.error(f'Could not fetch {post_href}')
            return None

        # Some pages are returned as markdown, some as HTML, so handle both
        if 'text/html' in article.headers.get('Content-Type'):
            return super().process_entry(post_href)

        return self._process_markdown(post_href, article)

    def _process_markdown(self, post_href, article):
        text = article.text
        metadata = self._get_metadata(text)
        
        return DataEntry({
            "source": self.name,
            "url": post_href,
            "title": metadata.get('title'),
            "authors": self.authors,
            "date_published": self._get_published_date(metadata),
            "text": text,
        })

    @staticmethod
    def _get_metadata(text):
        header = text.split('...')[0]
        def extract(item):
            parts = item.split(': ')
            if len(parts) > 1:
                return (parts[0], ': '.join(parts[1:]))
            return None

        return dict(filter(None, map(extract, header.splitlines())))

    def _get_article(self, url):
        logger.info("Fetching {}".format(url))
        return requests.get(url, allow_redirects=True)

    @staticmethod
    def _get_title(contents):
        return contents.find('header').find('h1').text


    @staticmethod
    def _get_published_date(metadata):
        date_published = metadata.get('modified') or metadata.get('created') or 'n/a'
        if date_published != 'n/a':
            date_published = datetime.strptime(date_published, "%Y-%m-%d").isoformat()
        return date_published


    def _get_text(self, contents):
        return self.cleaner.clean(contents.find('div', {'id': 'markdownBody'}).text)
