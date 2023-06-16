import datetime
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import requests
import jsonlines
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm
from markdownify import markdownify

from align_data.common.alignment_dataset import AlignmentDataset , DataEntry

logger = logging.getLogger(__name__)


def extract_author(base_url, a):
    return {
        'fullName': a.attrs.get('data-full-name'),
        'userId': a.attrs.get('data-userid'),
        'userLink': a.attrs.get('href') and base_url + a.attrs.get('href'),
        'name': a.text,
    }


def get_attr(elem: Tag, tag: str, selector, attr=None, processor=lambda x: x):
    """A generic extractor of HTML info, which will also handle the item not existing.

    :param Tag elem: the element to search in
    :param str tag: the HTML tag to look for, e.g. `div`. Can also be `None`, in which case any tag will work
    :param dict selector: additional selector to drill down on, e.g. `{'class': 'bla'}`
    :param str attr: the attribute of the element to extract, e.g. 'href'. Ignored if `None`
    :param fn processor: an optional transformer to be run on the extracted value for postprocessing
    """
    item = elem.find(tag, selector)
    value = item
    if attr and item:
        value = item and item.get(attr)
    return value and processor(value)


def parse_karma(meta_div: Tag):
    """Extract the karma from the given element.

    :param Tag meta_div: the element to be processed - this is the div containing url, karma, authors etc.
    :returns: a `(score, karma)` tuple, where `score` is the overall karma, while `karma` is a dict of per site karma
    """
    site = get_attr(meta_div, 'a', {'class': 'lw2-link'}, processor=lambda a: a.get('title') or next(a.children))
    karma_text = get_attr(meta_div, 'span', {'class': 'karma-value'}, processor=lambda d: d.text.strip())
    if not karma_text:
        score, karma = None, {}
    # In the case of this post only being on one server, the karma is provided as a string like "123 points"
    elif 'point' in karma_text:
        score = int(karma_text.split()[0].replace('−', '-'))
        karma = {site: score}
    # When it's e.g. an alignment forum post, it will have site specific karma, like "LW: 123, AF: 432"
    elif karma_text:
        parts = karma_text.replace(':', '').split()
        karma = {k: int(v) for k, v in zip(parts[::2], parts[1::2])}
        score = list(karma.values())[0]
    else:
        score, karma = None, {}
    return score, karma


def extract_metadata(base_url: str, post: Tag, meta_div=None):
    """Extract the metadata of the post/comment.

    :param str base_url: the base url of the forum being used, e.g. 'https://lesswrong.com'
    :param Tag post: the HTML element to process
    :param Tag meta_div: used if the metadata is in multiple tags. Will use `post` if `None`

    :returns: a dict of extracted metadata values. Values that are empty will be removed
    """
    meta_div = meta_div or post
    score, karma = parse_karma(meta_div)

    metadata = {
        'title': next(post.children).text,
        'url': get_attr(meta_div, 'a', {'class': 'lw2-link'}, 'href'),
        'post_url': get_attr(post, 'a', {'class': 'post-title-link'}, 'href', lambda url: base_url + url),
        'link_post': get_attr(post, 'a', {'class': 'link-post-link'}, 'href'),
        'authors': [extract_author(base_url, a) for a in meta_div.findChildren('a', {'class': 'author'})],
        'date_published': get_attr(
            meta_div, None, {'class': 'date'},
            processor=lambda d: datetime.datetime.strptime(d.text.strip(), '%d %b %Y %H:%M %Z').isoformat()
        ),
        'votes': get_attr(meta_div, 'span', {'class': 'karma-value'}, 'title', lambda v: int(v.split()[0])),
        'score': score,
        'karma': karma,
        'tags': get_attr(meta_div, 'div', {'id': 'tags'}, processor=lambda d: [a.text.strip() for a in d.find_all('a')]),
        'words': get_attr(meta_div, 'span', {'class': 'read-time'}, 'title'), #meta_div.find('span', {'class': 'read-time'}).attrs.get('title'),
    }
    return {k: v for k, v in metadata.items() if v}


def fetch_month_urls(base_url: str, year: int, month: int, delay=1):
    """Fetch all posts from the given `year` and `month` from `base_url`.

    This will automatically paginate through all available pages.
    GreaterWrong has a limit of 2000 entries per pagination, which is why this is done per month

    To avoid clobbering the service, `delay` seconds will be waited between each network call.

    :returns: a list of metadata dicts for each post
    """
    all_posts = []

    url = f'/archive/{year}/{month}'
    while url:
        logger.debug('Fetching items for %s', url)
        res = requests.get(base_url + url)
        soup = BeautifulSoup(res.text, "html.parser")

        posts = soup.find_all('h1', {'class': 'listing'})
        all_posts += [extract_metadata(base_url, post, post.find_next_sibling('div')) for post in posts]

        url = soup.find('a', {'class': 'nav-item-next'})
        url = url and url.attrs.get('href').replace('#', '')

        time.sleep(delay)

    logger.debug('Found %s posts for %s/%s', len(all_posts), year, month)
    return all_posts


def fetch_all_urls(base_url: str, urls_data_path: Path, start_year: int, delay=1):
    """Fetch the metadata of all posts from `base_url`, starting from `start_year`.

    This will create a separate data file for each month, starting from the earliest one checked. The resulting
    files will contain a JSON object per line containing the metadata of each post. If there were no posts in a
    give month (this happened in the beginning of LW, for example), then an empty file will be created to mark
    that month as checked. The latest month will always be rechecked, as it's most likely not up to date.
    """
    # Any url file that was created contains all urls for that month and so can be skipped. This
    # assumption only holds if post publication dates cannot be changed, and if posts won't retroactively
    # appear - both of which seem reasonable. Ignore the latest file though, as it probably won't contain
    # all urls for that given month
    known_urls = sorted(urls_data_path.glob('*'))[:-1]

    now = datetime.date.today()
    # Construct a big list of all months, rather than having nested loops, coz then
    # tqdm can show a nice loading bar
    dates = [
        (year, month)
        for year in range(start_year, now.year + 1)
        for month in range(1, 13)
    ]
    for year, month in tqdm(dates):
        data_file = urls_data_path / f'{year}_{month}.jsonl'

        if data_file in known_urls:
            logger.debug(f'Already processed {data_file.name} - skipping')
            continue

        try:
            posts = fetch_month_urls(base_url, year, month, delay)
        except Exception as e:
            logger.error(e)
        else:
            with jsonlines.open(data_file , mode='w') as writer:
                writer.write_all(posts)

        # No point in looking for future posts...
        if year == now.year and month == now.month:
            break


def parse_comments(base_url: str, elem: Tag):
    """Recursively extract the whole comment tree from the given HTML `elem`."""
    if not elem or not elem.get('class'):
        return None
    if 'comment-thread' in elem.get('class') or 'comments' in elem.get('class'):
        return list(filter(None, map(lambda x: parse_comments(base_url, x), elem.children)))
    if 'comment-item' in elem.get('class'):
        comment = elem.find('div', {'class': 'comment'})
        if 'deleted-comment' in comment.get('class'):
            return None

        metadata = extract_metadata(base_url, comment)

        return {
            'text': comment.find('div', {'class': 'body-text'}).text,
            'votes': metadata.get('votes'),
            'score': metadata.get('score'),
            'karma': metadata.get('karma'),
            'url': metadata.get('url'),
            'date_published': metadata['date_published'],
            'author': metadata.get('authors', [{}])[0].get('name'),
            'comments': parse_comments(base_url, elem.find('ul', {'class': 'comment-thread'})),
        }

    return None


def fetch_ai_tags(url):
    res = requests.get(url + '/tag/ai')
    soup = BeautifulSoup(res.content, "html.parser")
    container = soup.find('div', {'class': 'tag-description'}).find('table')
    return [a.text.strip() for a in container.find_all('a') if a.get('href').startswith('/tag/')]


@dataclass
class GreaterWrong(AlignmentDataset):

    """
    This class allows you to scrape posts and comments from GreaterWrong.
    GreaterWrong contains all the posts from LessWrong (which contains the Alignment Forum) and the EA Forum.
    """

    base_url: str
    start_year: int
    min_karma: int

    COOLDOWN_TIME : float = 0.5
    done_key = "url"

    def setup(self):
        super().setup()

        logger.info(f"Grabbing most recent links (grabs all links if /{self.name}/urls/ is empty)...")
        self.skipped_urls = self.raw_data_path / self.name / 'skipped'
        self.files_path = self.raw_data_path / self.name / 'urls'
        self.files_path.mkdir(parents=True, exist_ok=True)
        fetch_all_urls(self.base_url, self.files_path, self.start_year, self.COOLDOWN)

        logger.debug("Fetching ai tags...")
        try:
            self.ai_tags = set(fetch_ai_tags(self.base_url))
        except Exception:
            raise ValueError('Could not fetch tags! Please retry')

    @property
    def items_list(self):
        logger.debug("Converting each link to a json with post & comments...")
        if self.skipped_urls.exists():
            with open(self.skipped_urls) as f:
                skipped = {l.strip() for l in f}
        else:
            skipped = []

        links = []
        for filename in self.files_path.glob('*'):
            with jsonlines.open(filename) as reader:
                links += [
                    item for item in reader
                    if item.get('post_url') and item.get('score', 0) >= self.min_karma and item['post_url'] not in skipped
                ]
        return links

    def get_item_key(self, item):
        return item['url']

    def process_entry(self, item):
        # Skip this if the request failed. The idea being that the next scrape will pick it up
        post_url = item['post_url']
        try:
            res = requests.get(post_url)
        except requests.ConnectTimeout:
            logger.error('Timeout while fetching %s - skipping for now', post_url)
            return None

        if res.status_code != 200:
            logger.error('Got status code of %s while fetching %s - skipping for now', res.status_code, post_url)
            return None

        html = res.text.replace("\u201c", '"').replace("\u201d", '"')
        soup = BeautifulSoup(html, "html.parser")

        post = soup.find('main', {'class': 'post'})

        title = post.find('h1')
        meta_div = title.find_next_sibling('div')
        metadata = extract_metadata(self.base_url, title, meta_div)

        # Skip this item if it doesn't have at least one AI tag
        if not self.ai_tags & set(metadata.get('tags', [])):
            with open(self.skipped_urls, 'a') as f:
                f.write(post_url + '\n')
            return None

        return DataEntry(
            item,
            text=markdownify(post.find('div', {'class': 'body-text'}).renderContents()),
            comments=parse_comments(self.base_url, soup.find('div', {'id': 'comments'})),
            source=self.name,
            source_type='greaterwrong',
            **metadata
        )
