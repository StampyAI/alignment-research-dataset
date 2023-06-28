from dataclasses import dataclass
import re
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
from datetime import datetime, timezone

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
        authors = re.search(r"-by\s(.*)-date", filename.name).group(1)

        return DataEntry({
            "source": self.name,
            "source_type": "markdown",
            "title": title,
            "authors": [a.strip() for a in authors.split(',')],
            "date_published": self._get_published_date(filename),
            "text": text,
            "url": "n/a",
            "filename": filename.name,
        })
    
    @staticmethod
    def _get_published_date(filename):
        date_str = re.search(r"\d{4}-\d{2}-\d{2}", filename.name)
        
        if not date_str:
            return 'n/a'
        
        date_str = date_str.group(0)
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
