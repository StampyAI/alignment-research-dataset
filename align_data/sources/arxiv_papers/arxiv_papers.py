import logging
import re
from dataclasses import dataclass

import arxiv
from align_data.sources.articles.datasets import SpreadsheetDataset
from align_data.sources.articles.pdf import fetch_pdf, parse_vanity

logger = logging.getLogger(__name__)


@dataclass
class ArxivPapers(SpreadsheetDataset):
    summary_key: str = "summary"
    COOLDOWN: int = 1
    done_key = "url"
    batch_size = 1

    def _get_arxiv_metadata(self, paper_id) -> arxiv.Result:
        """
        Get metadata from arxiv
        """
        try:
            search = arxiv.Search(id_list=[paper_id], max_results=1)
            return next(search.results())
        except Exception as e:
            logger.error(e)
            return None

    def get_id(self, item):
        if res := re.search(r"https://arxiv.org/abs/(.*?)/?$", item.url):
            return res.group(1)

    def get_contents(self, item) -> dict:
        paper_id = self.get_id(item)
        for link in [
            f"https://www.arxiv-vanity.com/papers/{paper_id}",
            f"https://ar5iv.org/abs/{paper_id}",
        ]:
            if contents := parse_vanity(link):
                return contents
        return fetch_pdf(f"https://arxiv.org/pdf/{paper_id}.pdf")

    def process_entry(self, item) -> None:
        logger.info(f"Processing {item.title}")

        paper = self.get_contents(item)
        if not paper or not paper.get("text"):
            return None

        metadata = self._get_arxiv_metadata(self.get_id(item))
        if self.is_val(item.authors) and item.authors.strip():
            authors = item.authors.split(",")
        elif metadata and metadata.authors:
            authors = metadata.authors
        else:
            authors = paper.get("authors") or []
        authors = [str(a).strip() for a in authors]

        return self.make_data_entry(
            {
                "url": self.get_item_key(item),
                "source": self.name,
                "source_type": paper["data_source"],
                "title": self.is_val(item.title) or paper.get("title"),
                "authors": authors,
                "date_published": self._get_published_date(
                    self.is_val(item.date_published) or paper.get("date_published")
                ),
                "data_last_modified": str(metadata.updated),
                "summary": metadata.summary.replace("\n", " "),
                "author_comment": metadata.comment,
                "journal_ref": metadata.journal_ref,
                "doi": metadata.doi,
                "primary_category": metadata.primary_category,
                "categories": metadata.categories,
                "text": paper["text"],
            }
        )