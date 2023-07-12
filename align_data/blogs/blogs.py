from dataclasses import dataclass
from align_data import articles
from align_data.common.html_dataset import HTMLDataset, RSSDataset, AlignmentDataset
from align_data.articles.parsers import item_metadata

from dateutil.parser import parse, ParserError


class ColdTakes(HTMLDataset):
    item_selector = 'div.post-feed article'

    ignored_selectors = ['center', 'div[style*="display:flex"]', 'footer']

    def _get_published_date(self, contents):
        header = contents.select_one('article header').extract()
        date = header.find('time').get('datetime')
        return super()._get_published_date(date)


class GenerativeInk(HTMLDataset):
    item_selector = 'div.post.on-list'

    def _get_published_date(self, contents):
        possible_date_elements = [
            elem for info in contents.select('div.post-info')
            for elem in info.children
        ]
        return super()._get_published_date(self._find_date(possible_date_elements))


class CaradoMoe(RSSDataset):
    def _get_text(self, item):
        contents = item['soup']
        meta = contents.find('p', {'class': 'postmeta'})
        return self._extract_markdown(meta.find_next_sibling('div'))


class EleutherAI(HTMLDataset):

    item_selector = 'div.archive-entry'
    text_selector = 'div.post-content'

    def _get_published_date(self, contents):
        try:
            date = contents.select_one('header .post-meta').text.split('·')[0].strip()
            return super()._get_published_date(date)
        except (ValueError, ParserError):
            return ''

    def extract_authors(self, article):
        return article.select_one('header .post-meta').text.split('·')[1].strip().split(', ')


class OpenAIResearch(HTMLDataset):

    item_selector = 'li.group-item'
    title_selector = '.container h1'

    def _get_published_date(self, contents):
        if date := contents.select_one('.container .f-meta-2'):
            return super()._get_published_date(date.text)
        return ''

    def _get_text(self, contents):
        if paper_link := contents.select_one('.container .cols-container a.ui-link:-soup-contains("Read paper")'):
            return item_metadata(paper_link.get('href')).get('text')

    def extract_authors(self, article):
        authors = (
            article.select_one('div:-soup-contains("Authors") + div .f-body-1') or
            article.select_one('div:-soup-contains("Acknowledgments") + div .f-body-1')
        )
        if not authors:
            return []

        return [i.split('(')[0].strip() for i in authors.select_one('p').children if not i.name]
