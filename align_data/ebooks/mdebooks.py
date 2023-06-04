from dataclasses import dataclass
import re
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class MDEBooks(GdocDataset):

    done_key = "file_name"

    def setup(self):
        self._setup()
        if not self.files_path.exists():
            self.zip_from_gdrive()
        else:
            logger.info("Already downloaded")

    def fetch_entries(self):
        self.setup()
        for ii , filename in enumerate(tqdm(self.file_list)):
            if self._entry_done(filename):
                # logger.info(f"Already done {filename}")
                continue

            logger.info(f"Fetching {self.name} entry {filename}")
            text = filename.read_text()
            title = re.search(r"(.*)-by", filename.name, re.MULTILINE).group(1)
            date = re.search(r"\d{4}-\d{2}-\d{2}", filename.name).group(0)
            authors = re.search(r"-by\s(.*)-date", filename.name).group(1)

            new_entry = DataEntry({
                "source": self.name,
                "source_type": "markdown",
                "title": title,
                "authors": authors,
                "date_published": str(date),
                "text": text,
                "url": "n/a",
                "filename": filename.name,
            })
            new_entry.add_id()
            yield new_entry
