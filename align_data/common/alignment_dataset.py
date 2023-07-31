import logging
import time
import zipfile
from dataclasses import dataclass, field, KW_ONLY
from itertools import islice
from pathlib import Path
from typing import List
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import gdown
import jsonlines
import pytz
from dateutil.parser import parse, ParserError
from tqdm import tqdm
from align_data.db.models import Article
from align_data.db.session import make_session


INIT_DICT = {
    "source": None,
    "id": None,
    "text": None,
    "date_published": None,
    "title": None,
    "url": None,
    "authors": lambda: [],
}

logger = logging.getLogger(__name__)


@dataclass
class AlignmentDataset:
    """The base dataset class."""

    name: str
    """The name of the dataset"""

    _: KW_ONLY

    files_path = Path('')
    """The path where data can be found. Usually a folder"""

    done_key = 'id'

    """The key of the entry to use as the id when checking if already processed."""
    # Used to extract summaries - if `source_key` is set, the class will be deemed to collect summaries of other
    # articles.
    source_key = None
    """The key of the entry to use as an identifier of the article which it's summarizing - should be an URL"""
    summary_key = None
    """The key of the entry containing the summary contents. This is used both to get the summary, but also where
    it should be put in the target entry."""

    COOLDOWN = 0
    """An optional cool down between processing entries"""

    lazy_eval = False
    """Whether to lazy fetch items. This is nice in that it will start processing, but messes up the progress bar."""
    batch_size = 20
    """The number of items to collect before flushing to the database."""

    # Internal housekeeping variables
    _entry_idx = 0
    """Used internally for writing debugging info - each file write will increment it"""
    _outputted_items = set()
    """A set of the ids of all previously processed items"""
    _: KW_ONLY
    id_fields: List[str] = field(default_factory=lambda: ['url', 'title'])
    """A list of fields to use as the id of the entry. If not set, will use ['url', 'title']"""

    def __str__(self) -> str:
        return self.name

    def __post_init__(self, data_path=Path(__file__).parent / '../../data/'):
        self.data_path = data_path
        self.raw_data_path = self.data_path / 'raw'

        # set the default place to look for data
        self.files_path = self.raw_data_path / self.name

    def make_data_entry(self, data, **kwargs):
        data = dict(data, **kwargs)
        # TODO: Don't keep adding the same authors - come up with some way to reuse them
        # TODO: Prettify this
        data['authors'] = ','.join(data.get('authors', []))
        if summary := ('summary' in data and data.pop('summary')):
            data['summaries'] = [summary]
        return Article(
            id_fields=self.id_fields,
            meta={k: v for k, v in data.items() if k not in INIT_DICT},
            **{k: v for k, v in data.items() if k in INIT_DICT},
        )

    def to_jsonl(self, out_path=None, filename=None):
        if not out_path:
            out_path=Path(__file__).parent / '../../data/'

        if not filename:
            filename = f"{self.name}.jsonl"
        filename = Path(out_path) / filename

        with jsonlines.open(filename, 'w') as jsonl_writer:
            for article in self.read_entries():
                jsonl_writer.write(article.to_dict())
        return filename.resolve()

    def read_entries(self, sort_by=None):
        """Iterate through all the saved entries."""
        with make_session() as session:
            query = select(Article).where(Article.source==self.name)
            if sort_by is not None:
                query = query.order_by(sort_by)
            for item in session.scalars(query):
                yield item

    def add_entries(self, entries):
        def commit():
            try:
                session.commit()
                return True
            except IntegrityError:
                session.rollback()

        with make_session() as session:
            items = iter(entries)
            while batch := tuple(islice(items, self.batch_size)):
                session.add_all(batch)
                # there might be duplicates in the batch, so if they cause
                # an exception, try to commit them one by one
                if not commit():
                    for entry in batch:
                        session.add(entry)
                        if not commit():
                            logger.error(f'found duplicate of {entry}')

    def setup(self):
        self._outputted_items = self._load_outputted_items()

    @property
    def items_list(self):
        """Returns a collection of items to be processed."""
        return []

    def get_item_key(self, item):
        """Get the identifier of the given `item` so it can be checked to see whether it's been output.

        The default assumption is that the `item` is a Path to a file.
        """
        return item.name

    def _load_outputted_items(self):
        """Load the output file (if it exists) in order to know which items have already been output."""
        with make_session() as session:
            if hasattr(Article, self.done_key):
                return set(session.scalars(select(getattr(Article, self.done_key)).where(Article.source==self.name)).all())
            # TODO: Properly handle this - it should create a proper SQL JSON select
            return {item.get(self.done_key) for item in session.scalars(select(Article.meta).where(Article.source==self.name)).all()}

    def unprocessed_items(self, items=None):
        """Return a list of all items to be processed.

        This will automatically remove any items that have already been processed,
        based on the contents of the output file.
        """
        self.setup()

        def not_processed(item):
            return self.get_item_key(item) not in self._outputted_items

        filtered = filter(not_processed, items or self.items_list)

        # greedily fetch all items if not lazy eval. This makes the progress bar look nice
        if not self.lazy_eval:
            filtered = list(filtered)

        return tqdm(filtered, desc=f"Processing {self.name}")

    def fetch_entries(self):
        """Get all entries to be written to the file."""
        for item in self.unprocessed_items():
             entry = self.process_entry(item)
             if not entry:
                 continue

             yield entry

             if self.COOLDOWN:
                 time.sleep(self.COOLDOWN)

    def process_entry(self, entry):
        """Process a single entry."""
        raise NotImplementedError

    @staticmethod
    def _format_datetime(date):
        return date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_published_date(self, date):
        try:
            # Totally ignore any timezone info, forcing everything to UTC
            return parse(str(date)).replace(tzinfo=pytz.UTC)
        except ParserError:
            pass
        return None


@dataclass
class GdocDataset(AlignmentDataset):
    """A base Dataset handler for files that are saved on Gdrive,"""

    gdrive_address: str
    """The full URL to the gdrive file"""

    glob = '*.md'
    """How to identify files to be processed when going through a folder for files"""

    @property
    def items_list(self):
        """Returns a generator of items to be processed."""
        return self.files_path.glob(self.glob)

    @property
    def zip_file(self):
        """The name of the downloaded data, if a zip file."""
        return self.raw_data_path / f"{self.name}.zip"

    def zip_from_gdrive(self, url=None, filename=None, path=None):
        """Fetch the data a zip file from Gdrive.

        :param str url: the url to the file. Will use `self.gdrive_address` if empty
        :param str filename: the name of the zip file. Will use `self.zip_file` if empty
        :param str path: the path where the zip file should be extracted to. Will use `self.files_path` if empty
        """
        filename = filename or self.zip_file

        with open(filename, 'wb') as output:
            gdown.download(url=url or self.gdrive_address,
                           output=output,
                           quiet=False)

        logger.info("Unzipping")
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(path or self.files_path)

    def folder_from_gdrive(self, url=None, output=None):
        """Download a folder from gdrive.

        :param str url: the url to the file. Will use `self.gdrive_address` if empty
        :param str output: the path where the folder should be downloaded to. Will use `self.files_path` if empty
        """
        gdown.download_folder(
            url=url or self.gdrive_address,
            output=str(output or self.files_path),
            quiet=False
        )
