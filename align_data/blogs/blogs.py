from dataclasses import dataclass
from align_data.common.html_dataset import HTMLDataset, RSSDataset

from dateutil.parser import parse, ParserError


class ColdTakes(HTMLDataset):
    title_selector = 'h2'
    item_selector = 'div.post-feed article'

    ignored_selectors = ['center', 'div[style*="display:flex"]', 'footer']

    def _get_published_date(self, contents):
        header = contents.select_one('article header').extract()
        date = header.find('time').get('datetime')
        return self._format_datetime(parse(date))


class GenerativeInk(HTMLDataset):
    title_selector = 'h3'
    item_selector = 'div.post.on-list'

    def _get_published_date(self, contents):
        possible_date_elements = [
            elem for info in contents.select('div.post-info')
            for elem in info.children
        ]
        if date := self._find_date(possible_date_elements):
            return self._format_datetime(parse(date))
        return ''


class CaradoMoe(RSSDataset):
    def _get_text(self, item):
        contents = item['soup']
        meta = contents.find('p', {'class': 'postmeta'})
        return self._extract_markdown(meta.find_next_sibling('div'))


class EleutherAI(HTMLDataset):

    item_selector = 'div.archive-entry'
    title_selector = 'h4.archive-entry-title'
    text_selector = 'div.post-content'

    def _get_published_date(self, contents):
        try:
            date = contents.select_one('header .post-meta').text.split('·')[0].strip()
            return self._format_datetime(parse(date))
        except (ValueError, ParserError):
            return ''

    def extract_authors(self, article):
        return article.select_one('header .post-meta').text.split('·')[1].strip().split(', ')
