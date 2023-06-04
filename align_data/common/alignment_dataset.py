import hashlib
import os
import logging
from dataclasses import dataclass
from collections import UserDict
from pathlib import Path

import jsonlines
import gdown
import zipfile

INIT_DICT = {
    "source": None,
    "id": None,
    "text": None,
    "date_published": None,
    "title": None,
    "url": None,
}

TEXT_LEN = 1000

logger = logging.getLogger(__name__)


@dataclass
class AlignmentDataset:
    """The base dataset class."""

    name: str
    """The name of the dataset"""
    files_path = Path('')
    """The path where data can be found. Usually a folder"""
    done_ids = []
    """A collection of ids that have been processed - used internally to sort of provide idempotency"""
    done_key = None
    """The key of the entry to use as the id when checking if already processed. When `None` will use indexes"""

    glob = '*.md'
    """How to identify files to be processed when going through a folder for files"""

    def _setup(self) -> None:
        self.data_path = Path(__file__).parent / '../../data/'
        self.raw_data_path = self.data_path / 'raw'
        # make sure the path to the raw data exists
        self.raw_data_path.mkdir(parents=True, exist_ok=True)

        # set the default place to look for data
        self.files_path = self.raw_data_path / self.name

        self._mark_processed_items()

    def _mark_processed_items(self):
        """Load the output file (if it exists) in order to know which items have already been processed."""
        self.write_jsonl_path = self.data_path / f"{self.name}.jsonl"

        if not self.write_jsonl_path.exists():
            logger.info(f"No previous data found at {self.write_jsonl_path}")
            return None

        with jsonlines.open(self.write_jsonl_path, mode='r') as reader:
            if self.done_key:
                self.done_ids = [
                    (self.name, entry[self.done_key]) for entry in reader if self.done_key in entry
                ]
            else:
                self.done_ids = [(self.name, ii) for ii, entry in enumerate(reader)]

    def __str__(self) -> str:
        return f"{self.name} dataset will be written to {self.write_jsonl_path}"

    def _entry_done(self, entry):
        """
        Check if entry is already done
        """
        return (self.name, entry) in self.done_ids

    def fetch_entries(self):
        raise NotImplementedError

    def setup(self):
        raise NotImplementedError

    @property
    def file_list(self):
        """Returns a generator of files to be processed."""
        return self.files_path.glob(self.glob)


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
            gdown.download(url=url or self.gdrive_address, output=output, quiet=False)

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


class EntryWriter:
    def __init__(self, name, path, overwrite=False):
        """
        name: name of the blog, used as the file name
        path: path to save the blog posts
        """
        path = Path(path)

        # make sure the path exists
        path.mkdir(parents=True, exist_ok=True)

        jsonl_file = path / f'{name}.jsonl'
        txt_file = path / f'{name}.txt'

        write_mode = 'a' if not overwrite else 'w'
        self.jsonl_writer = jsonlines.open(jsonl_file, mode=write_mode)
        self.text_writer = open(txt_file, mode=write_mode)
        self.entry_idx = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.jsonl_writer.close()
        self.text_writer.close()

    def write(self, entry):
        # Save the entry in JSONL file
        self.jsonl_writer.write(entry.toJSON())

        # Save the entry in plain text, mainly for debugging
        print(f"[ENTRY {self.entry_idx}]", file=self.text_writer)
        text = '    '.join(('\n'+entry["text"].lstrip()).splitlines(True)) + '\n'
        print(text, file=self.text_writer)

        self.entry_idx += 1


class DataEntry(UserDict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in INIT_DICT.items():
            if k not in self:
                self[k] = v

    def add_id(self):
        assert self["text"] is not None, "Entry is missing text"
        text_excerpt = self["text"][:TEXT_LEN].encode("utf-8")
        self["id"] = hashlib.md5(text_excerpt).hexdigest()

    def _verify_id(self):
        assert self["id"] is not None, "Entry is missing id"
        assert self["text"] is not None, "Entry is missing text"
        text_excerpt = self["text"][:TEXT_LEN].encode("utf-8")
        assert self["id"] == hashlib.md5(
            text_excerpt).hexdigest(), "Entry id does not match text"

    def toJSON(self):
        for k, _ in INIT_DICT.items():
            assert self[k] is not None, f"Entry is missing key {k}"
        self._verify_id()
        return dict(self.data)
