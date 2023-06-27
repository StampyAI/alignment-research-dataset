from dataclasses import dataclass
import os
import pypandoc
import epub_meta
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
from path import Path

logger = logging.getLogger(__name__)

@dataclass
class GDrive(GdocDataset):
    """
    Pull .epubs from a Google Drive and convert them to .txt
    """

    done_key = "file_name"
    glob = '*.epub'

    def setup(self):
        super().setup()

        self.files_path = self.raw_data_path / 'books_text'

        if not self.files_path.exists():
            logger.info("Downloading everything...")
            self.files_path.mkdir(parents=True, exist_ok=True)
            self.folder_from_gdrive()

        self.weblink_pattern = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"

        self.pandoc_check_path = Path(os.getcwd()) / 'pandoc' / 'pandoc' #/ "/pandoc/pandoc"

        if self.pandoc_check_path.exists():
            logger.info("Make sure pandoc is configured correctly.")
            os.environ.setdefault("PYPANDOC_PANDOC", self.pandoc_check_path)

    def process_entry(self, epub_file):
        logger.info(f"Fetching {self.name} entry {epub_file.name}")
        try:
            text = pypandoc.convert_file(epub_file, "plain", extra_args=['--wrap=none'])
        except Exception as e:
            logger.error(f"Error converting {epub_file}")
            logger.error(e)
            text = "n/a"

        metadata = epub_meta.get_epub_metadata(epub_file)

        return DataEntry({
            "source": self.name,
            "source_filetype": "epub",
            "converted_with": "pandoc",
            "title": metadata["title"],
            "date_published": self._get_published_date(metadata),
            "chapter_names": [chap["title"] for chap in metadata["toc"]],
            "text": text,
            "url": "n/a",
            "file_name": epub_file.name,
            "authors": metadata['authors'],
        })

    @staticmethod
    def _get_published_date(metadata):
        date_published = metadata["publication_date"]
        return date_published or 'n/a'