from dataclasses import dataclass
import logging
import feedparser
from tqdm import tqdm

from align_data.common.html_dataset import RSSDataset


logger = logging.getLogger(__name__)


@dataclass
class WordpressBlog(RSSDataset):
    summary_key = 'summary'

    @property
    def feed_url(self):
        return self.url + "/feed"

    @property
    def items_list(self):
        logger.info(f"Fetching entries from {self.feed_url}")

        self.items = {}
        page_number = 1
        prev_title = None

        with tqdm(desc=f"Loading {self.name} pages") as pbar:
            while True:
                paged_url = f"{self.feed_url}?paged={page_number}"
                logging.info(f"Fetching {paged_url}")

                feed = feedparser.parse(paged_url)
                title = feed.get('feed', {}).get('title')
                if not title or title == prev_title:
                    break

                prev_title = feed["feed"]["title"]
                page_number += 1

                for item in feed['entries']:
                    self.items[item['link']] = item

                # update the tqdm progress bar
                pbar.set_postfix_str(f"page {page_number}", refresh=True)  # Set postfix to "page X"
                pbar.update()  # Here we increment the progress bar by 1

        logger.info(f'Got {len(self.items)} pages')
        return list(self.items.keys())
