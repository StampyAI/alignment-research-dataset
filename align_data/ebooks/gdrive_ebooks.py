from dataclasses import dataclass
import os
import pypandoc
import epub_meta
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
from path import Path
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class GDrive(GdocDataset):
    """
    Pull .epubs from a Google Drive and convert them to .txt
    """

    done_key = "file_name"

    def setup(self):
        self._setup()

        self.glob = '*.epub'
        self.files_path = self.raw_data_path / 'books_text'

        if not self.files_path.exists():
            logger.info("Downloading everything...")
            self.files_path.mkdir(parents=True, exist_ok=True)
            self.folder_from_gdrive()

        self.weblink_pattern = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"

        self.pandoc_check_path = Path(os.getcwd()) / "/pandoc/pandoc"

        if self.pandoc_check_path.exists():
            logger.info("Make sure pandoc is configured correctly.")
            os.environ.setdefault("PYPANDOC_PANDOC", self.pandoc_check_path)

    def fetch_entries(self):
        self.setup()
        for epub_file in tqdm(self.file_list):
            if self._entry_done(epub_file.name):
                # logger.info(f"Already done {epub_file}")
                continue

            logger.info(f"Fetching {self.name} entry {epub_file}")
            try:
                text = pypandoc.convert_file(epub_file, "plain", extra_args=['--wrap=none'])
            except Exception as e:
                logger.error(f"Error converting {epub_file}")
                logger.error(e)
                text = "n/a"

            metadata = epub_meta.get_epub_metadata(epub_file)

            new_entry = DataEntry({
                "source": "ebook",
                "source_filetype": "epub",
                "converted_with": "pandoc",
                "title": metadata["title"],
                "date_published": metadata["publication_date"] if metadata["publication_date"] else "n/a",
                "chapter_names": [chap["title"] for chap in metadata["toc"]],
                "text": text,
                "url": "n/a",
                "file_name": epub_file.name,
            })

            new_entry.add_id()
            yield new_entry
