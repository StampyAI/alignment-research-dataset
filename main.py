import os
from dataclasses import dataclass
from typing import List
import logging

import fire

from align_data import ALL_DATASETS, get_dataset
from align_data.analysis.count_tokens import count_token
from align_data.sources.articles.articles import update_new_items, check_new_articles
from align_data.pinecone.update_pinecone import ARDUpdater
from align_data.settings import (
    METADATA_OUTPUT_SPREADSHEET, METADATA_SOURCE_SHEET, METADATA_SOURCE_SPREADSHEET
)

logger = logging.getLogger(__name__)


@dataclass
class AlignmentDataset:

    out_path: str = "data"
    """The path to the directory where the data will be downloaded, defaults to data"""

    def list(self) -> List[str]:
        """Returns a list of all the datasets"""
        return sorted(ALL_DATASETS)

    def fetch(self, *names) -> None:
        """
        > This function takes a dataset name and writes the entries of that dataset to a file

        :param str name: The name of the dataset to fetch
        :return: The path to the file that was written to.
        """
        if names == ('all',):
            names = ALL_DATASETS
        missing = {name for name in names if name not in ALL_DATASETS}
        assert not missing, f"{missing} are not valid dataset names"
        for name in names:
            dataset = get_dataset(name)

            dataset.add_entries(dataset.fetch_entries())

    def fetch_all(self, *skip) -> None:
        """
        It downloads all the datasets, moves the alignment_newsletter.jsonl file to the processed
        folder, deletes the alignment_newsletter.jsonl file, adds the alignment_newsletter_summaries to
        the datasets, and merges all the files

        :param str|tuple skip: a comma separated list of datasources to be skipped
        :return: The path to the merged file.
        """
        names = [name for name in ALL_DATASETS if name not in skip]
        for name in names:
            print(name)
            self.fetch(name)

    def generate_jsonl_files(self, *names):
        """Generate jsonl files for the given datasets, on the basis of the database contents.

        :param List[str] names: The names of the datasets to generate
        """
        missing = {name for name in names if name not in ALL_DATASETS}
        assert not missing, f"{missing} are not valid dataset names"
        for name in names:
            dataset = get_dataset(name)
            print(dataset.to_jsonl())

    def count_tokens(self, merged_dataset_path: str) -> None:
        """
        This function counts the number of tokens, words, and characters in the dataset
        :return: None
        """
        assert os.path.exists(merged_dataset_path), "The path to the merged dataset does not exist"
        count_token(merged_dataset_path)

    def update_metadata(
            self, source_spreadsheet=METADATA_SOURCE_SPREADSHEET,
            source_sheet=METADATA_SOURCE_SHEET, output_spreadsheet=METADATA_OUTPUT_SPREADSHEET
    ):
        """Go through all unprocessed items from the source worksheet, updating the appropriate metadata in the output one.

        :param str source_spreadsheet: The id of the google docs spreadsheet containing the items to be processed
        :param str source_sheet: The name of the worksheet to be processed
        :param str output_spreadsheet: The id of the output google sheet where processed metadata should be added. This sheet should have a worksheet for each expected data type (e.g. "pdf", "html")
        """
        return update_new_items(source_spreadsheet, source_sheet, output_spreadsheet)

    def fetch_new_articles(self, source_spreadsheet=METADATA_SOURCE_SPREADSHEET, source_sheet=METADATA_SOURCE_SHEET):
        """Look for unseen articles in the special indices, adding any that are found to the provided spreadsheet.

        :param str source_spreadsheet: The id of the google docs spreadsheet containing the items to be processed
        :param str source_sheet: The name of the worksheet to be processed
        """
        return check_new_articles(source_spreadsheet, source_sheet)

    def update_pinecone(self):
        """
        This function updates the Pinecone vector DB.
        """
        updater = ARDUpdater()
        updater.update()

if __name__ == "__main__":
    fire.Fire(AlignmentDataset)
