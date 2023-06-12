from dataclasses import dataclass
import re
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class MDEBooks(GdocDataset):

    done_key = "filename"

    def setup(self):
        super().setup()
        if not self.files_path.exists():
            self.zip_from_gdrive()
        else:
            logger.info("Already downloaded")

    def process_entry(self, filename):
        logger.info(f"Fetching {self.name} entry {filename.name}")
        text = filename.read_text()
        title = re.search(r"(.*)-by", filename.name, re.MULTILINE).group(1)
        date = re.search(r"\d{4}-\d{2}-\d{2}", filename.name).group(0)
        authors = re.search(r"-by\s(.*)-date", filename.name).group(1)

        return DataEntry({
            "source": self.name,
            "source_type": "markdown",
            "title": title,
            "authors": authors,
            "date_published": str(date),
            "text": text,
            "url": "n/a",
            "filename": filename.name,
        })
