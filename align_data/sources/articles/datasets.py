import logging
from dataclasses import dataclass
from urllib.parse import urlparse

import pypandoc
import pandas as pd
from gdown.download import download
from markdownify import markdownify

from align_data.sources.articles.pdf import read_pdf
from align_data.sources.articles.parsers import HTML_PARSERS, extract_gdrive_contents
from align_data.sources.articles.google_cloud import fetch_markdown
from align_data.common.alignment_dataset import AlignmentDataset

logger = logging.getLogger(__name__)


@dataclass
class SpreadsheetDataset(AlignmentDataset):

    spreadsheet_id: str
    sheet_id: str
    done_key = "title"
    source_filetype = None
    batch_size = 1

    @staticmethod
    def is_val(val):
        if pd.isna(val):
            return None
        return val

    @property
    def items_list(self):
        logger.info(f'Fetching https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=CS&gid={self.sheet_id}')
        df = pd.read_csv(f'https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.sheet_id}')
        return (item for item in df.itertuples() if not pd.isna(self.get_item_key(item)))

    def get_item_key(self, item):
        return getattr(item, self.done_key)

    @staticmethod
    def _get_text(item):
        raise NotImplemented

    @staticmethod
    def extract_authors(item):
        if not SpreadsheetDataset.is_val(item.authors):
            return []
        return [author.strip() for author in item.authors.split(',') if author.strip()]

    def process_entry(self, item):
        text = self._get_text(item)
        if not text:
            logger.error('Could not get text for %s - skipping for now', item.title)
            return None

        return self.make_data_entry({
            'text': markdownify(text).strip(),
            'url': self.is_val(item.url),
            'title': self.is_val(item.title),
            'source': self.name,
            'source_type': self.is_val(item.source_type),
            'source_filetype': self.source_filetype,
            'date_published': self._get_published_date(item.date_published),
            'authors': self.extract_authors(item),
            'summary': self.is_val(item.summary),
        })


class PDFArticles(SpreadsheetDataset):

    source_filetype = 'pdf'
    COOLDOWN = 1
    batch_size = 1

    def _get_text(self, item):
        url = f'https://drive.google.com/uc?id={item.file_id}'

        filename = self.files_path / f'{item.title}.pdf'
        download(str(filename), id=item.file_id)
        return read_pdf(filename)


class HTMLArticles(SpreadsheetDataset):

    source_filetype = 'html'

    @staticmethod
    def _get_text(item):
        domain = urlparse(item.source_url).netloc.lstrip('www.')
        if parser := HTML_PARSERS.get(domain):
            return parser(item.source_url)


class EbookArticles(SpreadsheetDataset):

    source_filetype = 'epub'
    COOLDOWN = 10 # Add a large cooldown, as google complains a lot
    batch_size = 1

    def _get_text(self, item):
        file_id = item.source_url.split('/')[-2]
        filename = download(output=str(self.files_path / f'{item.title}.epub'), id=file_id)
        return pypandoc.convert_file(filename, "plain",'epub', extra_args=['--wrap=none'])


class XMLArticles(SpreadsheetDataset):

    source_filetype = 'xml'

    def _get_text(self, item):
        vals = extract_gdrive_contents(item.source_url)
        return vals['text']


class MarkdownArticles(SpreadsheetDataset):

    source_filetype = 'md'

    def _get_text(self, item):
        file_id = item.source_url.split('/')[-2]
        vals = fetch_markdown(file_id)
        return vals['text']
