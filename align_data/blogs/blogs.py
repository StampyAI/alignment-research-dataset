import regex as re
from dataclasses import dataclass
from markdownify import markdownify
from align_data.common import utils
from align_data.common.html_dataset import HTMLDataset, RSSDataset

from datetime import datetime, timezone
from dateutil.parser import parse

@dataclass
class ColdTakes(HTMLDataset):
    title_selector = 'h2'
    item_selector = 'div.post-feed article'

    cleaner = utils.HtmlCleaner(
        ["You might also like\.\.\..*", "\\n+", "\#\# Create your profile.*", "\n\xa0Comment/discuss\n", '\nClick lower right to download or find on Apple Podcasts, Spotify, Stitcher, etc.\n'],
        ["", "\\n", "", "", ''],
        True,
    )

    def _get_published_date(self, contents):
        article = contents.find('article')
        header = article.find('header').extract()
        date = header.find('time').get('datetime')
        return self._format_datetime(parse(date))


class GenerativeInk(HTMLDataset):
    title_selector = 'h3'
    item_selector = 'div.post.on-list'

    def _get_published_date(self, contents):
        possible_date_elements = [
            elem for info in contents.find_all('div', {'class': 'post-info'})
            for elem in info.children
        ]
        date = self._find_date(possible_date_elements)
        return self._format_datetime(parse(date))


class CaradoMoe(RSSDataset):
    def _get_text(self, item):
        contents = item['soup']
        meta = contents.find('p', {'class': 'postmeta'})
        return self._extract_markdown(meta.find_next_sibling('div'))
