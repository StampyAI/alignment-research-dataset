import re
import logging
from dataclasses import dataclass
from typing import List
from datetime import datetime, timezone
from align_data.common.alignment_dataset import GdocDataset, DataEntry

logger = logging.getLogger(__name__)


@dataclass
class MarkdownBlogs(GdocDataset):

    """
    Fetches articles from a blog where the posts are stored in markdown files on Google Drive.
    This is useful for blogs where the author posts about alignment, but many other things as well.
    Either store them manually yourself or ask them to send you markdown files of the post and store them in Gdrive.
    Useful tip: MarkDownload is a browser extension that makes it easy to grab posts and clean them quickly.
    If there are only a few dozen posts, it may be worth it to take 15 minutes to curate the alignment posts
    and store the markdowns in Gdrive.
    """

    authors: List[str]
    done_key = 'filename'

    def setup(self):
        super().setup()
        if not self.zip_file.exists():
            logger.info("Downloading scrape")
            self.zip_from_gdrive(path=self.raw_data_path)

        if (self.raw_data_path / f'{self.name}-cleaned-up').exists():
            self.files_path = self.raw_data_path / f'{self.name}-cleaned-up'
        else:
            self.files_path = self.raw_data_path / f'{self.name}'

    def process_entry(self, filename):
        text = filename.read_text()

        return DataEntry({
            "source": self.name,
            "source_type": "markdown",
            "title": self._get_title(filename),
            "authors": self.authors,
            "date_published": self._get_published_date(text),
            "text": text,
            "url": "n/a",
            'filename': filename.name,
        })
    
    @staticmethod
    def _get_published_date(text):
        date_str = re.search(r"^\d{4}-\d{2}-\d{2}", text, re.MULTILINE)
        
        if not date_str:
            return 'n/a'
            
        date_str = date_str.group(0)
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    @staticmethod
    def _get_title(filename):
        res = re.search(r"^#\s(.*)\n$", filename.read_text(), re.MULTILINE)
        if res:
            return res.group(1)    
        return filename.stem
