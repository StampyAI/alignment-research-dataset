from datetime import datetime
from align_data.blogs.html_blog import RSSBlog


class SubstackBlog(RSSBlog):
    source_type = "substack"

    @property
    def feed_url(self):
        return self.url + '/feed'

    def _get_contents(self, url):
        return self.items[url]

    @staticmethod
    def _get_text(item):
        return item.content and item.content[0] and item.content[0].get('value')

    @staticmethod
    def _get_published_date(item):
        return datetime.strptime(item['published'], '%a, %d %b %Y %H:%M:%S %Z').date().isoformat()

    @staticmethod
    def _get_title(item):
        return item['title']
