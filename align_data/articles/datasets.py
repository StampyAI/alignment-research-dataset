import time
import logging
from dataclasses import dataclass
from dateutil.parser import parse
from urllib.parse import urlparse

import requests
import pypandoc
import pandas as pd
from gdown.download import download

from align_data.articles.pdf import fetch_pdf, read_pdf, fetch
from align_data.articles.parsers import HTML_PARSERS
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

logger = logging.getLogger(__name__)


@dataclass
class SpreadsheetDataset(AlignmentDataset):

    spreadsheet_id: str
    sheet_id: str
    done_key = "title"

    @property
    def items_list(self):
        logger.info(f'Fetching https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=CS&gid={self.sheet_id}')
        df = pd.read_csv(f'https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.sheet_id}')
        return (item for item in df.itertuples() if not pd.isna(self.get_item_key(item)))

    def get_item_key(self, item):
        return getattr(item, self.done_key)

    def _get_published_date(self, item):
        return self._format_datetime(parse(item.date_published))

    @staticmethod
    def _get_text(item):
        raise NotImplemented

    @staticmethod
    def extract_authors(item):
        return [author.strip() for author in item.authors.split(',')]

    def process_entry(self, item):
        text = self._get_text(item)
        if not text:
            logger.error('Could not get text for %s - skipping for now', item.title)
            return None

        return DataEntry({
            'text': text,
            'url': item.url,
            'title': item.title,
            'source': self.name,
            'source_type': item.source_type,
            'source_filetype': 'pdf',
            'date_published': self._get_published_date(item),
            'authors': self.extract_authors(item),
            'summary': [] if pd.isna(item.summary) else [item.summary],
        })


class PDFArticles(SpreadsheetDataset):

    COOLDOWN = 1

    def _get_text(self, item):
        url = f'https://drive.google.com/uc?id={item.file_id}'

        filename = self.files_path / f'{item.title}.pdf'
        download(str(filename), id=item.file_id)
        return read_pdf(filename)


class HTMLArticles(SpreadsheetDataset):

    @staticmethod
    def _get_text(item):
        domain = urlparse(item.source_url).netloc.lstrip('www.')
        if parser := HTML_PARSERS.get(domain):
            return parser(item.source_url)


class EbookArticles(SpreadsheetDataset):

    COOLDOWN = 10 # Add a large cooldown, as google complains a lot

    def _get_text(self, item):
        file_id = item.source_url.split('/')[-2]
        filename = download(output=str(self.files_path / f'{item.title}.epub'), id=file_id)
        return pypandoc.convert_file(filename, "plain",'epub', extra_args=['--wrap=none'])
