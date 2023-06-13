import regex as re
from dataclasses import dataclass
from align_data.common import utils
from align_data.blogs.html_blog import HTMLBlog, RSSBlog


@dataclass
class ColdTakes(HTMLBlog):
    title_selector = 'h2'
    item_selector = ['article']

    cleaner = utils.HtmlCleaner(
        ["You might also like\.\.\..*", "\\n+", "\#\# Create your profile.*", "\n\xa0Comment/discuss\n", '\nClick lower right to download or find on Apple Podcasts, Spotify, Stitcher, etc.\n'],
        ["", "\\n", "", "", ''],
        True,
    )

    @staticmethod
    def _get_published_date(contents):
        article = contents.find('article')
        header = article.find('header').extract()
        return header.find('time').get('datetime')


class GenerativeInk(HTMLBlog):
    title_selector = 'h3'
    item_selector = ['div', {'class': 'post'}]

    def _get_published_date(self, contents):
        possible_date_elements = [
            elem for info in contents.find_all('div', {'class': 'post-info'})
            for elem in info.children
        ]
        return self._find_date(possible_date_elements)


class CaradoMoe(RSSBlog):
    @staticmethod
    def _get_text(contents):
        meta = contents.find('p', {'class': 'postmeta'})
        return meta.find_next_sibling('div').text

    @staticmethod
    def _get_published_date(contents):
        meta = contents.find('p', {'class': 'postmeta'})
        date = re.search('\d{4}-\d{2}-\d{2}', meta.text)
        if date:
            return date.group(0)
        return 'n/a'

    @staticmethod
    def _get_title(contents):
        return contents.find('h2').text
