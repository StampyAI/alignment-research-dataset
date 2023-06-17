from dataclasses import dataclass
import os
import fire
from dataclasses import dataclass
from typing import List, Union
import align_data
from align_data.analysis.count_tokens import count_token

# import logging , sys

# logging.basicConfig(stream=sys.stdout, level=logging.INFO)


@dataclass
class AlignmentDataset:

    out_path: str = "data"
    """The path to the directory where the data will be downloaded, defaults to data"""

    def list(self) -> List[str]:
        """Returns a list of all the datasets"""
        return sorted(align_data.ALL_DATASETS)

    def fetch(self, name, rebuild=False) -> None:
        """
        > This function takes a dataset name and writes the entries of that dataset to a file

        :param str name: The name of the dataset to fetch
        :param bool rebuild: Whether to remove the previous build before running
        :return: The path to the file that was written to.
        """
        assert name in align_data.ALL_DATASETS, f"{name} is not a valid dataset name"
        dataset = align_data.get_dataset(name)

        if rebuild:
            dataset.jsonl_path.unlink(missing_ok=True)

        with dataset.writer(self.out_path) as writer:
            for entry in dataset.fetch_entries():
                writer(entry)

        return dataset.jsonl_path

    def fetch_all(self, rebuild=False) -> str:
        """
        It downloads all the datasets, moves the alignment_newsletter.jsonl file to the processed
        folder, deletes the alignment_newsletter.jsonl file, adds the alignment_newsletter_summaries to
        the datasets, and merges all the files

        :param bool rebuild: Whether to remove the previous build before running
        :return: The path to the merged file.
        """
        for name in align_data.ALL_DATASETS:
            print(name)
            self.fetch(name, rebuild)

        return None  #merge_all_files(out_dir = self.out_path)

    def count_tokens(self, merged_dataset_path: str) -> None:
        """
        This function counts the number of tokens, words, and characters in the dataset
        :return: None
        """
        assert os.path.exists(merged_dataset_path), "The path to the merged dataset does not exist"
        count_token(merged_dataset_path)


if __name__ == "__main__":
    fire.Fire(AlignmentDataset)
