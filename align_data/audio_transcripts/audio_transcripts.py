from dataclasses import dataclass
import gdown
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import zipfile
import os
import logging
import re
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class AudioTranscripts(GdocDataset):

    done_key = 'filename'

    def setup(self):
        super().setup()

        self.files_path = self.raw_data_path / 'transcripts'
        if not self.files_path.exists():
            self.files_path.mkdir(parents=True, exist_ok=True)
            self.zip_from_gdrive(path=self.raw_data_path)

    def process_entry(self, filename):
        logger.info(f"Processing {filename.name}")
        text = filename.read_text()
        title = filename.stem

        date = re.search(r"\d{4}\d{2}\d{2}", str(filename)).group(0)
        date = date[:4] + "-" + date[4:6] + "-" + date[6:]

        return DataEntry({
            "source": self.name,
            "source_filetype": "audio",
            "url": "n/a",
            "converted_with": "otter-ai",
            "title": title,
            "authors": "unknown",
            "date_published": str(date),
            "text": text,
            'filename': filename.name,
        })
