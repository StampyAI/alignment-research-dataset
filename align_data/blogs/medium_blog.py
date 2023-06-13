from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
import bs4
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry
import logging
from urllib.parse import urljoin
from markdownify import markdownify
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class MediumBlog(AlignmentDataset):
    """
    Fetches articles from a Medium blog.

    Pulls Medium articles by walking the archive. Depending on the activity of the blog
    during a particular year, the archive for the year may consist of a single page only, or
    may have daily pages. A single blog can use different layouts for different years.

    Also, if the blog had few posts overall, an archive may not exist at all. In that case,
    the main page is used to fetch the articles. The entries are assumed to fit onto
    a single page, which seems to be the case for blogs without an archive.

    It is possible that there is additional variation in the layout that hasn't been represented
    in the blogs tested so far. In that case, additional fixes to this code may be needed.

    This implementation was originally based on
    https://dorianlazar.medium.com/scraping-medium-with-python-beautiful-soup-3314f898bbf5,
    but various fixes were added to handle a wider range of Medium blogs.
    """

    url: str
    done_key = "url"

    def get_item_key(self, item):
        article_url = item.find_all("a")[0]["href"].split("?")[0]
        return urljoin(self.url, article_url)

    @property
    def items_list(self):
        logger.info(f"Fetching entries from {self.url}")
        response = requests.get(self.url, allow_redirects=True)
        soup = BeautifulSoup(response.content, "html.parser")
        articles = soup.find_all("article")
        logger.info(f"Found {len(articles)} articles")
        return articles

    def process_entry(self, article):
        title = article.find("h2")
        if title is None:
            return None
        title = title.contents[0]

        article_url = self.get_item_key(article)

        logger.info(f"Processing {title}")

        text = self._get_article(article_url)

        return DataEntry({
            "source": self.url,
            "source_type": "medium_blog",
            "url": article_url,
            "title": self._to_text(title),
            "date_published": "n/a",
            "text": text,
        })

    def _to_text(self, s):
        if type(s) is bs4.element.Tag:
            return s.text
        return s

    def _get_article(self, url):
        logger.info("Fetching {}".format(url))
        article = requests.get(url, allow_redirects=True)
        return markdownify(article.content)
