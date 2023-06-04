from dataclasses import dataclass
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
import pypandoc
from path import Path
import os
import docx
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class Gdocs(GdocDataset):

    done_key = "docx_name"

    def setup(self):
        self._setup()
        self.glob = "*.docx"

        self.zip_from_gdrive()

        self.pandoc_check_path = Path(os.getcwd()) / "/pandoc/pandoc"

        if self.pandoc_check_path.exists():
            logger.info("Make sure pandoc is configured correctly.")
            os.environ.setdefault("PYPANDOC_PANDOC", self.pandoc_check_path)

    def fetch_entries(self):
        self.setup()
        for docx_filename in tqdm(self.file_list):
            if self._entry_done(docx_filename.name):
                # logger.info(f"Already done {docx_filename}")
                continue

            logger.info(f"Fetching {self.name} entry {docx_filename}")
            try:
                text = pypandoc.convert_file(docx_filename, "plain", extra_args=['--wrap=none'])
            except Exception as e:
                logger.error(f"Error converting {docx_filename}")
                logger.error(e)
                text = "n/a"

            metadata = self._get_metadata(docx_filename)

            new_entry = DataEntry({
                "source": self.name,
                "source_filetype": "docx",
                "converted_with": "pandoc",
                "title": metadata.title,
                "authors": metadata.author,
                "date_published": metadata.created if metadata.created else "n/a",
                "text": text,
                "url": "n/a",
                "docx_name": docx_filename.name,
            })

            new_entry.add_id()
            yield new_entry

    def _get_metadata(self , docx_filename):
        doc = docx.Document(docx_filename)
        return doc.core_properties
