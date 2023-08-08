import logging

import requests
from align_data.sources.articles.parsers import item_metadata
from align_data.common.html_dataset import HTMLDataset, RSSDataset
from bs4 import BeautifulSoup
from dateutil.parser import ParserError
from tqdm import tqdm

logger = logging.getLogger(__name__)


class ColdTakes(HTMLDataset):
    item_selector = "div.post-feed article"

    ignored_selectors = ["center", 'div[style*="display:flex"]', "footer"]

    def _get_published_date(self, contents):
        header = contents.select_one("article header").extract()
        date = header.find("time").get("datetime")
        return super()._get_published_date(date)


class GenerativeInk(HTMLDataset):
    item_selector = "div.post.on-list"

    def _get_published_date(self, contents):
        possible_date_elements = [
            elem for info in contents.select("div.post-info") for elem in info.children
        ]
        return self._find_date(possible_date_elements)


class CaradoMoe(RSSDataset):
    def _get_text(self, item):
        contents = item["soup"]
        meta = contents.find("p", {"class": "postmeta"})
        return self._extract_markdown(meta.find_next_sibling("div"))


class EleutherAI(HTMLDataset):
    item_selector = "div.archive-entry"
    text_selector = "div.post-content"

    def _get_published_date(self, contents):
        try:
            date = contents.select_one("header .post-meta").text.split("·")[0].strip()
            return super()._get_published_date(date)
        except (ValueError, ParserError):
            return ""

    def extract_authors(self, article):
        return (
            article.select_one("header .post-meta")
            .text.split("·")[1]
            .strip()
            .split(", ")
        )


class OpenAIResearch(HTMLDataset):
    item_selector = "li.group-item"
    title_selector = ".container h1"

    def _get_published_date(self, contents):
        if date := contents.select_one(".container .f-meta-2"):
            return super()._get_published_date(date.text)
        return ""

    def _get_text(self, contents):
        if paper_link := contents.select_one(
            '.container .cols-container a.ui-link:-soup-contains("Read paper")'
        ):
            return item_metadata(paper_link.get("href")).get("text")

    def extract_authors(self, article):
        authors = article.select_one(
            'div:-soup-contains("Authors") + div .f-body-1'
        ) or article.select_one('div:-soup-contains("Acknowledgments") + div .f-body-1')
        if not authors:
            return []

        return [
            i.split("(")[0].strip()
            for i in authors.select_one("p").children
            if not i.name
        ]


class DeepMindTechnicalBlog(HTMLDataset):
    item_selector = "div.w-dyn-item .c_card_list__item__blog"
    title_selector = ".c_banner__blog__card h2"
    text_selector = ".c_rich-text__cms"
    ignored_selectors = [".article-gtag-buttons"]

    @property
    def items_list(self):
        articles = []
        page = 1
        with tqdm(desc=f"Loading {self.name} pages") as pbar:
            while True:
                logger.info(f"Fetching entries from {self.url}")
                response = requests.get(
                    self.url, allow_redirects=True, params={"73df3071_page": page}
                )
                soup = BeautifulSoup(response.content, "html.parser")
                items = soup.select(self.item_selector)
                if not items:
                    break
                articles += items

                page += 1

                # update the tqdm progress bar
                pbar.set_postfix_str(
                    f"page {page}", refresh=True
                )  # Set postfix to "page X"
                pbar.update()  # Here we increment the progress bar by 1

        logger.info("Got %s pages", len(articles))
        return articles

    def _get_published_date(self, contents):
        if date := contents.select_one(".c_banner__blog__card__meta"):
            return super()._get_published_date(date.text)
        return ""

    def extract_authors(self, article):
        if div := article.select_one(
            '.c_cms_content__meta__wrapper div:-soup-contains("Authors") + div'
        ):
            return [author.strip() for author in div.text.split(",")]
        return []
