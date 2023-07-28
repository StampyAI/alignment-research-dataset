from dataclasses import dataclass
import os
from datetime import datetime, timezone

import docx
from dateutil.parser import parse
from path import Path #TODO: replace with pathlib
import pypandoc

from logger_config import logger
from align_data.common.alignment_dataset import GdocDataset


@dataclass
class Gdocs(GdocDataset):

    done_key = "docx_name"
    glob = "*.docx"

    def setup(self):
        super().setup()

        self.zip_from_gdrive()

        self.pandoc_check_path = Path(os.getcwd()) / "/pandoc/pandoc"

        if self.pandoc_check_path.exists():
            logger.info("Make sure pandoc is configured correctly.")
            os.environ.setdefault("PYPANDOC_PANDOC", self.pandoc_check_path)

    def process_entry(self, docx_filename):
        logger.info(f"Fetching {self.name} entry {docx_filename}")
        try:
            text = pypandoc.convert_file(docx_filename, "plain", extra_args=['--wrap=none'])
            metadata = self._get_metadata(docx_filename)
        except Exception as e:
            logger.error(f"Error processing {docx_filename}")
            logger.error(e)
            return None

        return self.make_data_entry({
            "source": self.name,
            "source_type": "docx",
            "converted_with": "pandoc",
            "title": metadata.title,
            "authors": [metadata.author] if metadata.author else [],
            "date_published": self._get_published_date(metadata),
            "text": text,
            "url": "",
            "docx_name": docx_filename.name,
        })
    
    @staticmethod
    def _get_published_date(metadata):
        date_published = metadata.created or metadata.modified
        if date_published:
            assert isinstance(date_published, datetime), f"Expected datetime, got {type(date_published)}"
            dt = date_published.replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return ''


    def _get_metadata(self , docx_filename):
        doc = docx.Document(docx_filename)
        return doc.core_properties
