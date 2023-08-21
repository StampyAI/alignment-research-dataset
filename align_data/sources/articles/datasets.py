import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

import pandas as pd
from align_data.sources.articles import articles
from gdown.download import download
from markdownify import markdownify
from pypandoc import convert_file
from sqlalchemy import select

from align_data.common.alignment_dataset import AlignmentDataset
from align_data.db.models import Article
from align_data.sources.articles.google_cloud import fetch_file, fetch_markdown
from align_data.sources.articles.parsers import (
    HTML_PARSERS, extract_gdrive_contents, item_metadata, parse_domain
)
from align_data.sources.articles.pdf import read_pdf
from align_data.sources.arxiv_papers import fetch_arxiv, canonical_url as arxiv_cannonical_url

logger = logging.getLogger(__name__)


@dataclass
class SpreadsheetDataset(AlignmentDataset):
    spreadsheet_id: str
    sheet_id: str
    done_key = "url"
    source_filetype = None
    batch_size = 1

    @staticmethod
    def maybe(item, key: str):
        val = getattr(item, key, None)
        if pd.isna(val):
            return None
        return val

    def get_item_key(self, item):
        return self.maybe(item, self.done_key)

    @property
    def items_list(self):
        url = f'https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.sheet_id}'
        logger.info(f'Fetching {url}')
        df = pd.read_csv(url)
        return (item for item in df.itertuples() if self.get_item_key(item))

    @staticmethod
    def _get_text(item):
        raise NotImplementedError

    @staticmethod
    def extract_authors(item):
        if not SpreadsheetDataset.maybe(item, "authors"):
            return []
        return [author.strip() for author in item.authors.split(",") if author.strip()]

    def process_entry(self, item):
        text = self._get_text(item)
        if not text:
            logger.error("Could not get text for %s - skipping for now", item.title)
            return None

        url = self.maybe(item, "url")
        source_url = self.maybe(item, "source_url")

        return self.make_data_entry(
            {
                "text": markdownify(text).strip(),
                "url": url,
                "source_url": source_url if source_url != url else None,
                "title": self.maybe(item, "title"),
                "source": self.name,
                "source_type": self.maybe(item, "source_type"),
                "source_filetype": self.source_filetype,
                "date_published": self._get_published_date(item.date_published),
                "authors": self.extract_authors(item),
                "summary": self.maybe(item, "summary"),
            }
        )


class SpecialDocs(SpreadsheetDataset):

    @property
    def _query_items(self):
        special_docs_types = ["pdf", "html", "xml", "markdown", "docx"]
        return select(Article).where(Article.source.in_(special_docs_types))

    def get_contents(self, item) -> Dict:
        contents = {}
        if url := self.maybe(item, "source_url") or self.maybe(item, "url"):
            contents = item_metadata(url)

        return dict(contents, **{
            'url': self.maybe(item, "url"),
            'title': self.maybe(item, "title") or contents.get('title'),
            'source': contents.get('source_type') or self.name,
            'source_url': self.maybe(item, "source_url"),
            'source_type': contents.get('source_type') or self.maybe(item, "source_type"),
            'date_published': self._get_published_date(self.maybe(item, 'date_published')) or contents.get('date_published'),
            'authors': self.extract_authors(item) or contents.get('authors', []),
            'text': contents.get('text'),
            'status': 'Invalid' if contents.get('error') else None,
            'comments': contents.get('error'),
        })

    def not_processed(self, item):
        url = self.maybe(item, 'url')
        source_url = self.maybe(item, 'source_url')

        return (
            self.get_item_key(item) not in self._outputted_items and
            url not in self._outputted_items and
            source_url not in self._outputted_items and
            (not url or arxiv_cannonical_url(url) not in self._outputted_items) and
            (not source_url or arxiv_cannonical_url(source_url) not in self._outputted_items)
        )

    def process_entry(self, item):
        if ArxivPapers.is_arxiv(item.url):
            contents = ArxivPapers.get_contents(item)
            contents['source'] = 'arxiv'
        else:
            contents = self.get_contents(item)

        return self.make_data_entry(contents)


class PDFArticles(SpreadsheetDataset):
    source_filetype = "pdf"
    COOLDOWN = 1
    batch_size = 1

    def setup(self):
        super().setup()
        self.files_path.mkdir(exist_ok=True, parents=True)

    def _get_text(self, item):
        filename = self.files_path / f"{item.title}.pdf"
        if download(output=str(filename), id=item.file_id):
            return read_pdf(filename)


class HTMLArticles(SpreadsheetDataset):
    source_filetype = "html"

    @staticmethod
    def _get_text(item):
        domain = parse_domain(item.source_url)
        if parser := HTML_PARSERS.get(domain):
            res = parser(item.source_url)
            return res and res.get('text')


class EbookArticles(SpreadsheetDataset):
    source_filetype = "epub"
    COOLDOWN = 10  # Add a large cooldown, as google complains a lot
    batch_size = 1

    def setup(self):
        super().setup()
        self.files_path.mkdir(exist_ok=True, parents=True)

    def _get_text(self, item):
        file_id = item.source_url.split("/")[-2]
        filename = download(
            output=str(self.files_path / f"{item.title}.epub"), id=file_id
        )
        return convert_file(filename, "plain", "epub", extra_args=["--wrap=none"])


class XMLArticles(SpreadsheetDataset):
    source_filetype = "xml"

    def _get_text(self, item):
        vals = extract_gdrive_contents(item.source_url)
        return vals.get("text")


class MarkdownArticles(SpreadsheetDataset):
    source_filetype = "markdown"

    def _get_text(self, item):
        file_id = item.source_url.split("/")[-2]
        vals = fetch_markdown(file_id)
        return vals.get("text")


class DocArticles(SpreadsheetDataset):
    source_filetype = "docx"

    def setup(self):
        super().setup()
        self.files_path.mkdir(exist_ok=True, parents=True)

    def _get_text(self, item):
        pandoc_path = Path("data/raw/pandoc/pandoc/")
        if pandoc_path.exists():
            logger.info("Make sure pandoc is configured correctly.")
            os.environ.setdefault("PYPANDOC_PANDOC", str(pandoc_path))

        file_id = item.source_url.split("/")[-2]
        file_name = fetch_file(file_id)
        return convert_file(file_name, "md", format="docx", extra_args=["--wrap=none"])


class ArxivPapers(SpreadsheetDataset):
    COOLDOWN: int = 1

    @staticmethod
    def is_arxiv(url):
        return parse_domain(url) == "arxiv.org"

    @classmethod
    def get_contents(cls, item) -> Dict:
        contents = fetch_arxiv(item.url or item.source_url)

        if cls.maybe(item, "authors") and item.authors.strip():
            contents['authors'] = [i.strip() for i in item.authors.split(',')]
        if cls.maybe(item, "title"):
            contents['title'] = cls.maybe(item, "title")

        contents['date_published'] = cls._get_published_date(
            cls.maybe(item, "date_published") or contents.get('date_published')
        )
        return contents

    def process_entry(self, item):
        logger.info(f"Processing {item.title}")

        return self.make_data_entry(self.get_contents(item), source=self.name)
