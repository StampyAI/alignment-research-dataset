import re
from typing import Any, Dict
from bs4 import BeautifulSoup

from align_data.common.html_dataset import RSSDataset
from align_data.sources.articles.parsers import item_metadata
from align_data.sources.utils import merge_dicts


class AGISFPodcastDataset(RSSDataset):
    regex = re.compile(r"^\[Week .*?\]\s+“(?P<title>.*?)”\s+by\s+(?P<authors>.*?)$")

    @property
    def feed_url(self):
        return self.url

    def fetch_contents(self, url: str) -> Dict[str, Any]:
        contents = super().fetch_contents(url)
        if extracted := self.regex.search(contents.get("title")):
            return merge_dicts(contents, extracted.groupdict())
        return contents

    def _get_text(self, item):
        contents = item_metadata(item["link"])
        # Replace any non empty values in item. `item.update()` will happily insert Nones
        for k, v in contents.items():
            if v:
                item[k] = v
        return item.get("text")

    def extract_authors(self, item):
        authors = item.get("authors")
        if not authors:
            return self.authors
        if isinstance(authors, str):
            return [a.strip() for a in authors.split(",")]
        return authors

    def _extra_values(self, contents):
        if summary := contents.get("summary"):
            soup = BeautifulSoup(summary, "html.parser")
            for el in soup.select("b"):
                if el.next_sibling:
                    el.next_sibling.extract()
                el.extract()
            return {"summary": self._extract_markdown(soup)}
        return {}

    def process_entry(self, article):
        article_url = self.get_item_key(article)
        contents = self.get_contents(article_url)

        return self.make_data_entry(contents)
