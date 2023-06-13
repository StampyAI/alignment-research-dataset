from dataclasses import dataclass
import requests
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


@dataclass
class GwernBlog(AlignmentDataset):
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
            'https://www.gwern.net/Complexity-vs-AI.page',
            'https://www.gwern.net/Tool-AI.page',
            'https://www.gwern.net/Backstop.page',
            'https://www.gwern.net/Hyperbolic-Time-Chamber.page'
        ]

    def process_entry(self, post_href):
        text = self._get_article(post_href)

        return DataEntry({
            "source": "gwern",
            "url": post_href,
            "title": text.splitlines()[1].split("title: ")[1],
            "authors": "Gwern Branwen",
            "date_published": "n/a",
            "text": text,
        })

    def _get_article(self, url):
        logger.info("Fetching {}".format(url))
        article = requests.get(url, allow_redirects=True)
        return article.text
