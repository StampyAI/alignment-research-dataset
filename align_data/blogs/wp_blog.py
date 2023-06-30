from datetime import datetime, timezone
from calendar import c
from dataclasses import dataclass, field
import logging
from tqdm import tqdm
import feedparser

from align_data.common import utils
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

from typing import List

logger = logging.getLogger(__name__)

@dataclass
class WordpressBlog(AlignmentDataset):
    url: str
    strip: List = field(default_factory=lambda: [])
    max_pages: int = 2000
    summary_key = 'summary'
    done_key = 'paged_url'

    def setup(self):
        """
        url: URL of the blog
        strip: list of regexes to strip from the HTML
        max_pages: maximum number of RSS pages to fetch
        """
        super().setup()
        self.feed_url = self.url + "/feed"
        self.cleaner = utils.HtmlCleaner(self.strip)
        self.max_pages = self.max_pages
        self.name = utils.url_to_filename(self.url)

    def get_item_key(self, item):
        return item

    @property
    def items_list(self):
        return [f"{self.feed_url}?paged={page + 1}" for page in range(0, self.max_pages)]

    @staticmethod
    def _get_published_date(item):
        date_published = item.get('published')
        if not date_published:
            return ''
        date_published = datetime.strptime(date_published, '%a, %d %b %Y %H:%M:%S %z')
        return self._format_datetime(parse(date_published))

    def fetch_entries(self):
        last_title = ""
        for paged_url in self.unprocessed_items():
            logger.info(f"Fetching {paged_url} (max={self.max_pages})")
            d = feedparser.parse(paged_url)

            if (
                ("feed" not in d)
                or ("title" not in d["feed"])
                or (d["feed"]["title"] == last_title)
            ):
                logger.info(
                    "Not a valid page. It looks like we've reached the end.")
                break

            last_title = d["feed"]["title"]

            for entry in d["entries"]:
                content_text = self.cleaner.clean(entry["content"][0]["value"])
                text = entry["title"] + "\n\n" + content_text

                new_entry = DataEntry({
                    "text": text,
                    "url": entry['link'],
                    "title": text.split("\n")[0],
                    "source": self.name,
                    "source_type": "blog",
                    "date_published": self._get_published_date(entry),
                    "paged_url": paged_url,
                    "authors": [e['name'] for e in entry.get('authors', [])],
                })
                new_entry.add_id()

                yield new_entry
