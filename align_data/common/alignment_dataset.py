import hashlib
import logging
import time
import zipfile
from collections import UserDict
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import gdown
import jsonlines
import pytz
from dateutil.parser import parse
from tqdm import tqdm

INIT_DICT = {
    "source": None,
    "id": None,
    "text": None,
    "date_published": None,
    "title": None,
    "url": None,
    "summary": lambda: [],
    "authors": lambda: [],
}

# Used to limit the size of the text used when generating hashes.
# TODO: Why is this even needed? It doesn't seem likely that any individual entry
# will be hundreds of MB large, and if not, then why bother with limiting the length of
# text for hashing? Speed might be an issue, but I'm guessing that I/O, especially network
# stuff, will be a much larger problem.
# One possible reason could be dynamic http sites etc. But that's all the more reason to check
# the whole text, rather than just the header...
TEXT_LEN = -1

logger = logging.getLogger(__name__)


@dataclass
class AlignmentDataset:
    """The base dataset class."""

    name: str
    """The name of the dataset"""
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

    glob = '*.md'
    """How to identify files to be processed when going through a folder for files"""

    COOLDOWN = 0
    """An optional cool down between processing entries"""

    lazy_eval = False
    """Whether to lazy fetch items. This is nice in that it will start processing, but messes up the progress bar."""

    # Internal housekeeping variables
    _entry_idx = 0
    """Used internally for writing debugging info - each file write will increment it"""
    _outputted_items = set()
    """A set of the ids of all previously processed items"""

    def __str__(self) -> str:
        return f"{self.name} dataset will be written to {self.jsonl_path}"

    def __post_init__(self, data_path=Path(__file__).parent / '../../data/'):
        self.data_path = data_path
        self.raw_data_path = self.data_path / 'raw'

        # set the default place to look for data
        self.files_path = self.raw_data_path / self.name

        # and the default place to write data
        self._set_output_paths(self.data_path)

    def _set_output_paths(self, out_path):
        self.jsonl_path = Path(out_path) / f"{self.name}.jsonl"
        self.txt_path = Path(out_path) / f"{self.name}.txt"

    def write_entry(self, entry, jsonl_writer, text_writer):
        jsonl_writer.write(entry.to_dict())

        # Save the entry in plain text, mainly for debugging
        text = entry["text"].lstrip().replace('\n', '\n    ')
        text_writer.write(f'[ENTRY {self._entry_idx}]\n    {text}\n\n')

        self._entry_idx += 1
        self._outputted_items.add(entry[self.done_key])

    @contextmanager
    def writer(self, out_path=None, overwrite=False):
        """Returns a function that can be used to write entries to the output file.

        The resulting function expects to only get a single `DataEntry`, which will then
        be written as a json object.
        """
        if overwrite:
            write_mode = 'w'
            self._entry_idx = 0
        else:
            write_mode = 'a'

        if out_path:
            self._set_output_paths(out_path)

        with jsonlines.open(self.jsonl_path, mode=write_mode) as jsonl_writer:
            with open(self.txt_path, mode=write_mode, errors="backslashreplace") as text_writer:
                yield partial(self.write_entry, jsonl_writer=jsonl_writer, text_writer=text_writer)

    def read_entries(self):
        """Iterate through all the saved entries."""
        if not self.jsonl_path.exists():
            return []

        with jsonlines.open(self.jsonl_path) as f:
            for line in f:
                yield line

    def merge_summaries(self, summaries):
        if not self.summary_key or not self.jsonl_path.exists():
            return

        updated = 0
        tmp_file = self.jsonl_path.parent / f'{self.jsonl_path.name}-tmp'
        with jsonlines.open(tmp_file, 'w') as writer:
            for line in self.read_entries():
                url = line.get('url')
                summary = summaries.get(url, {})
                line[self.summary_key] += list(summary.values())
                updated += bool(summary)
                writer.write(line)

        logger.info('Updated %s summaries for %s', updated, self.name)
        tmp_file.rename(self.jsonl_path)

    def setup(self):
        # make sure the path to the raw data exists
        self.files_path.mkdir(parents=True, exist_ok=True)

        self._outputted_items = self._load_outputted_items()

    @property
    def items_list(self):
        """Returns a generator of items to be processed."""
        return self.files_path.glob(self.glob)

    def get_item_key(self, item):
        """Get the identifier of the given `item` so it can be checked to see whether it's been output.

        The default assumption is that the `item` is a Path to a file.
        """
        return item.name

    def _load_outputted_items(self):
        """Load the output file (if it exists) in order to know which items have already been output."""
        if not self.jsonl_path.exists():
            logger.info(f"No previous data found at {self.jsonl_path}")
            return set()

        with jsonlines.open(self.jsonl_path, mode='r') as reader:
            return {entry.get(self.done_key) for entry in reader}

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

             entry.add_id()
             yield entry

             if self.COOLDOWN:
                 time.sleep(self.COOLDOWN)

    def process_entry(self, entry):
        """Process a single entry."""
        raise NotImplementedError

    @staticmethod
    def _format_datetime(date):
        # Totally ignore any timezone info, forcing everything to UTC
        dt = date.replace(tzinfo=pytz.UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _get_published_date(self, date):
        if date:
            return self._format_datetime(parse(date))
        return ''


@dataclass
class GdocDataset(AlignmentDataset):
    """A base Dataset handler for files that are saved on Gdrive,"""

    gdrive_address: str
    """The full URL to the gdrive file"""

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


class DataEntry(UserDict):
    def __init__(self, *args, id_fields=None, **kwargs):
        super().__init__(*args, **kwargs)
        for k, default in INIT_DICT.items():
            if k not in self:
                self[k] = default and default()
        # Store id_fields in a way that does not interfere with UserDict's functionality
        self.__id_fields = id_fields or ["url", "title"]

    def generate_id_string(self):
        return ''.join(str(self[field]) for field in self.__id_fields).encode("utf-8")

    def verify_fields(self):
        missing = [field for field in self.__id_fields if self.get(field) is None]
        assert not missing, f'Entry is missing the following fields: {missing}'

        id_string = self.generate_id_string()
        assert id_string, "Entry has empty id_fields"
        return id_string
        
    def add_id(self):
        id_string = self.verify_fields()
        self["id"] = hashlib.md5(id_string).hexdigest()

    def _verify_id(self):
        assert self["id"] is not None, "Entry is missing id"
        id_string = self.verify_fields()

        id_from_fields = hashlib.md5(id_string).hexdigest()
        assert self["id"] == id_from_fields, f"Entry id {self['id']} does not match id from id_fields, {id_from_fields}"

    def to_dict(self):
        for k, _ in INIT_DICT.items():
            assert self[k] is not None, f"Entry is missing key {k}"
        self._verify_id()
        return dict(self.data)
