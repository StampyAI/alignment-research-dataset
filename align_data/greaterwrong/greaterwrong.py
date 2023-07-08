from datetime import datetime, timezone
from dateutil.parser import parse
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import requests
import jsonlines
from bs4 import BeautifulSoup
from tqdm import tqdm
from markdownify import markdownify

from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

logger = logging.getLogger(__name__)


def fetch_LW_tags(url):
    res = requests.get(
        url + '/tag/ai',
        headers={'User-Agent': 'Mozilla /5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0'},
    )
    soup = BeautifulSoup(res.content, "html.parser")
    tags = soup.select('div.TagPage-description .table a')
    return {a.text.strip() for a in tags if '/tag/' in a.get('href')}


def fetch_ea_forum_topics(url):
    res = requests.get(url + '/topics/ai-safety')
    soup = BeautifulSoup(res.content, "html.parser")
    links = soup.select('div.SidebarSubtagsBox-root a')
    return {a.text.strip() for a in links if '/topics/' in a.get('href', '')}


def get_allowed_tags(url, name):
    if name == 'alignmentforum':
        return set()
    try:
        if name == 'lesswrong':
            return fetch_LW_tags(url)
        if name == 'eaforum':
            return fetch_ea_forum_topics(url)
    except Exception:
        raise ValueError('Could not fetch tags! Please retry')

    raise ValueError(f'Could not fetch tags for unknown datasource: "{name}". Must be one of alignmentforum|lesswrong|eaforum')


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
    af: bool
    """Whether alignment forum posts should be returned"""

    limit = 50
    COOLDOWN_TIME : float = 0.5
    summary_key: str = 'summary'
    done_key = "url"
    lazy_eval = True

    def setup(self):
        super().setup()

        logger.info(f"Grabbing most recent links (grabs all links if /{self.name}/urls/ is empty)...")
        self.skipped_urls = self.raw_data_path / self.name / 'skipped'

        logger.debug("Fetching ai tags...")
        self.ai_tags = get_allowed_tags(self.base_url, self.name)

    def tags_ok(self, post):
        return not self.ai_tags or {t['name'] for t in post['tags']} & self.ai_tags

    def get_item_key(self, item):
        return item['pageUrl']

    @staticmethod
    def _get_published_date(item):
        date_published = item['postedAt']
        if date_published:
            dt = parse(date_published).astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return ''

    def make_query(self, after: str):
        return """{
            posts(input: {
            terms: {
                excludeEvents: true
                view: "old"
        """ \
        f"      af: {self.af}\n" \
        f"      limit: {self.limit}\n" \
        f"      karmaThreshold: {self.min_karma}\n" \
        f'        after: "{after}"\n' \
        """        filter: "tagged"
            }
            }) {
            totalCount
            results {
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
                  tags {
                  name
                }
                user {
                  displayName
                }
                coauthors {
                  displayName
                }
                af
                htmlBody
            }
            }
        }"""

    def fetch_posts(self, query: str):
        res = requests.post(
            f'{self.base_url}/graphql',
            # The GraphQL endpoint returns a 403 if the user agent isn't set... Makes sense, but is annoying
            headers={'User-Agent': 'Mozilla /5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0'},
            json={'query': query}
        )
        return res.json()['data']['posts']

    @property
    def items_list(self):
        next_date = datetime(self.start_year, 1, 1).isoformat() + 'Z'
        if self.jsonl_path.exists() and self.jsonl_path.lstat().st_size:
            with jsonlines.open(self.jsonl_path) as f:
                for item in f:
                    if item['date_published'] > next_date:
                        next_date = item['date_published']

        logger.info('Starting from %s', next_date)
        while next_date:
            posts = self.fetch_posts(self.make_query(next_date))
            if not posts['results']:
                return

            for post in posts['results']:
                if post['htmlBody'] and self.tags_ok(post):
                    yield post

            next_date = posts['results'][-1]['postedAt']
            time.sleep(self.COOLDOWN)

    def process_entry(self, item):
        authors = item['coauthors']
        if item['user']:
            authors = [item['user']] + authors
        authors = [a['displayName'] for a in authors]
        return DataEntry({
            'title': item['title'],
            'text': markdownify(item['htmlBody']),
            'url': item['pageUrl'],
            'date_published': self._get_published_date(item),
            'modified_at': item['modifiedAt'],
            "source": self.name,
            "source_type": "GreaterWrong",
            'votes': item['voteCount'],
            'karma': item['baseScore'],
            'tags': [t['name'] for t in item['tags']],
            'words': item['wordCount'],
            'comment_count': item['commentCount'],
            # Some posts don't have authors, for some reaason
            'authors': authors,
        })
