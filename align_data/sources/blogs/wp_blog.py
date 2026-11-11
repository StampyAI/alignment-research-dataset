from dataclasses import dataclass
import logging

import requests
from tqdm import tqdm

from align_data.common.html_dataset import RSSDataset, fetch_feed

logger = logging.getLogger(__name__)


@dataclass
class WordpressBlog(RSSDataset):

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
                logger.info(f"Fetching {paged_url}")

                try:
                    feed = fetch_feed(paged_url)
                except requests.exceptions.HTTPError as e:
                    # WordPress answers /feed?paged=N past the last page with a 404,
                    # so that is the end of pagination. A 404 on the first page means
                    # the feed URL itself is broken, and anything else is a real error.
                    if page_number > 1 and e.response is not None and e.response.status_code == 404:
                        logger.info(
                            f"{self.name}: page {page_number} returned 404, stopping pagination"
                        )
                        break
                    raise
                title = feed.get("feed", {}).get("title")
                if not title or title == prev_title:
                    break

                prev_title = feed["feed"]["title"]
                page_number += 1

                for item in feed["entries"]:
                    self.items[item["link"]] = item

                # update the tqdm progress bar
                pbar.set_postfix_str(f"page {page_number}", refresh=True)  # Set postfix to "page X"
                pbar.update()  # Here we increment the progress bar by 1

        logger.info(f"Got {len(self.items)} pages")
        return list(self.items.keys())
