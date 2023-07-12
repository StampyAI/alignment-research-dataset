import arxiv
import requests
import logging
import time
import jsonlines

import pandas as pd

from dataclasses import dataclass
from markdownify import markdownify
from bs4 import BeautifulSoup
from tqdm import tqdm
from align_data.common.alignment_dataset import AlignmentDataset

logger = logging.getLogger(__name__)


@dataclass
class ArxivPapers(AlignmentDataset):
    summary_key: str = 'summary'
    COOLDOWN: int = 1
    done_key = "url"

    def _get_arxiv_metadata(self, paper_id) -> arxiv.Result:
        """
        Get metadata from arxiv
        """
        try:
            search = arxiv.Search(id_list=[paper_id], max_results=1)
        except Exception as e:
            logger.error(e)
            return None
        return next(search.results())

    @property
    def items_list(self):
        self.papers_csv_path = self.raw_data_path / "ai-alignment-arxiv-papers.csv"

        self.df = pd.read_csv(self.papers_csv_path)
        self.df_arxiv = self.df[self.df["Url"].str.contains(
            "arxiv.org/abs") == True].drop_duplicates(subset="Url", keep="first")

        return [xx.split('/abs/')[1] for xx in self.df_arxiv.Url]

    def process_entry(self, ids) -> None:
        logger.info(f"Processing {ids}")

        markdown = self.process_id(ids)

        paper = self._get_arxiv_metadata(ids)
        if markdown is None or paper is None:
            logger.info(f"Skipping {ids}")
            return None
        else:
            new_entry = self.make_data_entry({
                "url": self.get_item_key(ids),
                "source": self.name,
                "source_type": "html",
                "converted_with": "markdownify",
                "title": paper.title,
                "authors": [str(x) for x in paper.authors],
                "date_published": paper.published.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data_last_modified": str(paper.updated),
                "abstract": paper.summary.replace("\n", " "),
                "author_comment": paper.comment,
                "journal_ref": paper.journal_ref,
                "doi": paper.doi,
                "primary_category": paper.primary_category,
                "categories": paper.categories,
                "text": markdown,
            })
        return new_entry


    def _is_bad_soup(self, soup, parser='vanity') -> bool:
        if parser == 'vanity':
            vanity_wrapper = soup.find("div", class_="arxiv-vanity-wrapper")
            if vanity_wrapper is None:
                return None
            vanity_wrapper = vanity_wrapper.text
            return vanity_wrapper and "don’t have to squint at a PDF" not in vanity_wrapper
        if parser == 'ar5iv':
            ar5iv_error = soup.find("span", class_="ltx_ERROR")
            if ar5iv_error is None: 
                return False
            else: 
                ar5iv_error = ar5iv_error.text
            if "document may be truncated or damaged" in ar5iv_error:
                return True
        return False


    def _is_dud(self, markdown) -> bool:
        """
        Check if markdown is a dud
        """
        return (
            "Paper Not Renderable" in markdown or 
            "This document may be truncated" in markdown or 
            "don’t have to squint at a PDF" not in markdown
        )

    def _article_markdown_from_soup(self, soup):
        """
        Get markdown of the article from BeautifulSoup object of the page
        """
        article = soup.article
        if article is None:
            return None
        article = self._remove_bib_from_article_soup(article)
        markdown = markdownify(str(article))
        return markdown


    def _get_parser_markdown(self, paper_id, parser="vanity") -> str:
        """
        Get markdown from the parser website, arxiv-vanity or ar5iv.org
        """
        if parser == "vanity":
            link = f"https://www.arxiv-vanity.com/papers/{paper_id}"
        elif parser == "ar5iv":
            link = f"https://ar5iv.org/abs/{paper_id}"
        logger.info(f"Fetching {link}")
        try:
            r = requests.get(link, timeout=5 * self.COOLDOWN)
        except ValueError as e:
            logger.error(f'{e}')
            return None
        if "//arxiv.org" in r.url:
            return None
        try:
            soup = BeautifulSoup(r.content, features="xml")
        except ValueError as e:
            logger.error(f'{e}')
            return None
        if not self._is_bad_soup(soup,parser=parser):
            return self._article_markdown_from_soup(soup)
        return None


    def get_item_key(self, paper_id) -> str:
        """
        Get arxiv link
        """
        return f"https://arxiv.org/abs/{paper_id}"

    def _remove_bib_from_article_soup(self, article_soup) -> str:
        """
        Strip markdown
        """
        bib = article_soup.find("section", id="bib")
        if bib:
            bib.decompose()
        return article_soup
        
    def _strip_markdown(self, s_markdown):
        return s_markdown.split("\nReferences\n")[0].replace("\n\n", "\n")

    def process_id(self, paper_id) -> str:
        """
        Process arxiv id
        """
        markdown = self._get_parser_markdown(paper_id, parser="vanity")
        if markdown is None:
            markdown = self._get_parser_markdown(paper_id, parser="ar5iv")
        if markdown is None:
            return None
        mardown_excerpt = markdown.replace('\n', '')[:100]
        logger.info(f"Stripping markdown, {mardown_excerpt}")
        s_markdown = self._strip_markdown(markdown)
        return s_markdown
