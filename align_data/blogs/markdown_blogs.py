from dataclasses import dataclass
import re
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
from tqdm import tqdm

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

    done_key = None

    def setup(self):
        self._setup()
        if not self.zip_file.exists():
            logger.info("Downloading scrape")
            self.zip_from_gdrive(path=self.raw_data_path)

        if (self.raw_data_path / f'{self.name}-cleaned-up').exists():
            self.files_path = self.raw_data_path / f'{self.name}-cleaned-up'
        else:
            self.files_path = self.raw_data_path / f'{self.name}'

    def fetch_entries(self):
        self.setup()
        for ii, filename in enumerate(tqdm(self.file_list)):
            if self._entry_done(ii):
                # logger.info(f"Already done {ii} , {filename}")
                continue
            with open(filename , "r") as f:
                text = f.read()

            try:
                title = re.search(r"^#\s(.*)\n$", text, re.MULTILINE).group(1)
                date = re.search(r"^\d{4}-\d{2}-\d{2}",
                                 text, re.MULTILINE).group(0)
            except:
                title, date = filename.stem, "n/a"

            new_entry = DataEntry({
                "source": self.name,
                "source_type": "markdown",
                "title": title,
                "authors": "n/a",
                "date_published": str(date),
                "text": text,
                "url": "n/a",
            })

            new_entry.add_id()
            yield new_entry
