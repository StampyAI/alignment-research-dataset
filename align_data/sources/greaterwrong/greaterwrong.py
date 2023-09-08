from datetime import datetime
import logging
import time
from dataclasses import dataclass
from typing import Set, Tuple

import requests
from bs4 import BeautifulSoup
from markdownify import markdownify
from sqlalchemy import select

from align_data.common.alignment_dataset import AlignmentDataset
from align_data.db.session import make_session
from align_data.db.models import Article

logger = logging.getLogger(__name__)


def fetch_LW_tags(url):
    res = requests.get(
        url + "/tag/ai",
        headers={
            "User-Agent": "Mozilla /5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0"
        },
    )
    soup = BeautifulSoup(res.content, "html.parser")
    tags = soup.select("div.TagPage-description .table a")
    return {a.text.strip() for a in tags if "/tag/" in a.get("href")}


def fetch_ea_forum_topics(url):
    res = requests.get(url + "/topics/ai-safety")
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select("div.SidebarSubtagsBox-root a")
    return {a.text.strip() for a in links if "/topics/" in a.get("href", "")}


def get_allowed_tags(url, name):
    if name == "alignmentforum":
        return set()
    try:
        if name == "lesswrong":
            return fetch_LW_tags(url)
        if name == "eaforum":
            return fetch_ea_forum_topics(url)
    except Exception:
        raise ValueError("Could not fetch tags! Please retry")

    raise ValueError(
        f'Could not fetch tags for unknown datasource: "{name}". Must be one of alignmentforum|lesswrong|eaforum'
    )


@dataclass
class GreaterWrong(AlignmentDataset):

    """
    This class allows you to scrape posts and comments from GreaterWrong.
    GreaterWrong contains all the posts from LessWrong (which contains the Alignment Forum) and the EA Forum.
    """

    base_url: str
    start_year: int
    min_karma: int
    """Posts must have at least this much karma to be returned."""
    af: bool = False
    """Whether alignment forum posts should be returned"""

    limit = 50
    COOLDOWN_TIME : float = 0.5
    done_key = "url"
    lazy_eval = True
    source_type = 'GreaterWrong'
    _outputted_items = (set(), set())

    def setup(self):
        super().setup()

        logger.debug("Fetching ai tags...")
        self.ai_tags = get_allowed_tags(self.base_url, self.name)

    def tags_ok(self, post):
        return not self.ai_tags or {t["name"] for t in post["tags"] if t.get("name")} & self.ai_tags

    def _load_outputted_items(self) -> Tuple[Set[str], Set[Tuple[str, str]]]:
        """Load the output file (if it exists) in order to know which items have already been output."""
        with make_session() as session:
            articles = (
                session
                .query(Article.url, Article.title, Article.authors)
                .where(Article.source_type == self.source_type)
                .all()
            )
            return (
                {a.url for a in articles},
                {(a.title.replace('\n', '').strip(), a.authors) for a in articles},
            )

    def not_processed(self, item):
        title = item["title"]
        url = item["pageUrl"]
        authors = ','.join(self.extract_authors(item))

        return (
            url not in self._outputted_items[0]
            and (title, authors) not in self._outputted_items[1]
        )

    def _get_published_date(self, item):
        return super()._get_published_date(item.get("postedAt"))

    def make_query(self, after: str):
        return f'''
        {{
            posts(input: {{
                terms: {{
                    excludeEvents: true
                    view: "old"
                    af: {self.af}
                    limit: {self.limit}
                    karmaThreshold: {self.min_karma}
                    after: "{after}"
                    filter: "tagged"
                }}
            }}) {{
                totalCount
                results {{
                    _id
                    title
                    slug
                    pageUrl
                    postedAt
                    modifiedAt
                    score
                    extendedScore
                    baseScore
                    voteCount
                    commentCount
                    wordCount
                    tags {{
                        name
                    }}
                    user {{
                        displayName
                    }}
                    coauthors {{
                        displayName
                    }}
                    af
                    htmlBody
                }}
            }}
        }}
        '''

    def fetch_posts(self, query: str):
        res = requests.post(
            f"{self.base_url}/graphql",
            # The GraphQL endpoint returns a 403 if the user agent isn't set... Makes sense, but is annoying
            headers={
                "User-Agent": "Mozilla /5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0"
            },
            json={"query": query},
        )
        return res.json()["data"]["posts"]

    @property
    def last_date_published(self) -> str:
        entries = self.read_entries(sort_by=Article.date_published.desc())
        
        # Get the first entry if exists, else return a default datetime
        prev_item = next(entries, None)
        
        # If there is no previous item or it doesn't have a published date, return default datetime
        if not prev_item or not prev_item.date_published:
            return datetime(self.start_year, 1, 1).isoformat() + 'Z'

        # If the previous item has a published date, return it in isoformat
        return prev_item.date_published.isoformat() + 'Z'

    @property
    def items_list(self):
        next_date = self.last_date_published
        logger.info("Starting from %s", next_date)
        while next_date:
            posts = self.fetch_posts(self.make_query(next_date))
            if not posts["results"]:
                return

            for post in posts["results"]:
                if post["htmlBody"] and self.tags_ok(post):
                    yield post

            next_date = posts["results"][-1]["postedAt"]
            time.sleep(self.COOLDOWN)

    def extract_authors(self, item):
        authors = item["coauthors"]
        if item["user"]:
            authors = [item["user"]] + authors
        # Some posts don't have authors, for some reaason
        return [a["displayName"] for a in authors] or ["anonymous"]

    def process_entry(self, item):
        return self.make_data_entry(
            {
                "title": item["title"],
                "text": markdownify(item["htmlBody"]).strip(),
                "url": item["pageUrl"],
                "date_published": self._get_published_date(item),
                "modified_at": item["modifiedAt"],
                "source": self.name,
                "source_type": self.source_type,
                "votes": item["voteCount"],
                "karma": item["baseScore"],
                "tags": [t["name"] for t in item["tags"]],
                "words": item["wordCount"],
                "comment_count": item["commentCount"],
                "authors": self.extract_authors(item),
            }
        )
