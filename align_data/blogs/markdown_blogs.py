import re
import logging
from dataclasses import dataclass
from typing import List
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
        try:
            title = re.search(r"^#\s(.*)\n$", text, re.MULTILINE).group(1)
            date = re.search(r"^\d{4}-\d{2}-\d{2}", text, re.MULTILINE).group(0)
        except:
            title, date = filename.stem, "n/a"

        return DataEntry({
            "source": self.name,
            "source_type": "markdown",
            "title": title,
            "authors": self.authors,
            "date_published": str(date),
            "text": text,
            "url": "n/a",
            'filename': filename.name,
        })
