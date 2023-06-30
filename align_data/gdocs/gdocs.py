from dataclasses import dataclass
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
import pypandoc
from path import Path
import os
import docx
from datetime import datetime, timezone
from dateutil.parser import parse

logger = logging.getLogger(__name__)

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
        except Exception as e:
            logger.error(f"Error converting {docx_filename}")
            logger.error(e)
            text = "n/a"

        metadata = self._get_metadata(docx_filename)

        return DataEntry({
            "source": self.name,
            "source_type": "docx",
            "converted_with": "pandoc",
            "title": metadata.title,
            "authors": [metadata.author] if metadata.author else [],
            "date_published": self._get_published_date(metadata),
            "text": text,
            "url": "n/a",
            "docx_name": docx_filename.name,
        })
    
    @staticmethod
    def _get_published_date(metadata):
        date_published = metadata.created or metadata.modified
        if date_published:
            assert isinstance(date_published, datetime), f"Expected datetime, got {type(date_published)}"
            dt = date_published.replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return 'n/a'


    def _get_metadata(self , docx_filename):
        doc = docx.Document(docx_filename)
        return doc.core_properties
