import os
import fire
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Union
from align_data import ALL_DATASETS, DATASET_REGISTRY, get_dataset
from align_data.analysis.count_tokens import count_token

# import logging , sys

# logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def add_summaries(summaries, dataset):
    for line in dataset.read_entries():
        url = line.get(dataset.source_key)
        summary = line.get(dataset.summary_key)
        if url and summary:
            summaries[url][dataset.name] = summary
    return summaries


@dataclass
class AlignmentDataset:

    out_path: str = "data"
    """The path to the directory where the data will be downloaded, defaults to data"""

    def list(self) -> List[str]:
        """Returns a list of all the datasets"""
        return sorted(ALL_DATASETS)

    def fetch(self, name, rebuild=False) -> None:
        """
        > This function takes a dataset name and writes the entries of that dataset to a file

        :param str name: The name of the dataset to fetch
        :param bool rebuild: Whether to remove the previous build before running
        :return: The path to the file that was written to.
        """
        assert name in ALL_DATASETS, f"{name} is not a valid dataset name"
        dataset = get_dataset(name)

        if rebuild:
            dataset.jsonl_path.unlink(missing_ok=True)

        with dataset.writer(self.out_path) as writer:
            for entry in dataset.fetch_entries():
                writer(entry)

        return dataset.jsonl_path

    def fetch_all(self, rebuild=False, skip='') -> str:
        """
        It downloads all the datasets, moves the alignment_newsletter.jsonl file to the processed
        folder, deletes the alignment_newsletter.jsonl file, adds the alignment_newsletter_summaries to
        the datasets, and merges all the files

        :param bool rebuild: Whether to remove the previous build before running
        :params str|tuple skip: a comma separated list of datasources to be skipped
        :return: The path to the merged file.
        """
        for name in ALL_DATASETS:
            if name in skip:
                continue
            print(name)
            self.fetch(name, rebuild)

        return None  #merge_all_files(out_dir = self.out_path)

    def merge_summaries(self, *names):
        """Update all source materials with summaries if they have any.

        Some datasets are actual alignment content, e.g. arXiv articles, while other datasets are mainly
        summaries of other articles, e.g. the alignment newsletter. This command merges the two, adding all
        summaries to all found entries. In theory it's possible for a single article to have multiple different
        summaries, therefore the summaries are added as a dict of <source>: <summary>
        """
        summaries = defaultdict(lambda: dict())
        for dataset in DATASET_REGISTRY:
            if dataset.source_key and dataset.summary_key:
                add_summaries(summaries, dataset)

        if names:
            datasets = [get_dataset(name) for name in names]
        else:
            datasets = DATASET_REGISTRY

        for dataset in datasets:
            if not dataset.source_key and dataset.summary_key:
                dataset.merge_summaries(summaries)

    def count_tokens(self, merged_dataset_path: str) -> None:
        """
        This function counts the number of tokens, words, and characters in the dataset
        :return: None
        """
        assert os.path.exists(merged_dataset_path), "The path to the merged dataset does not exist"
        count_token(merged_dataset_path)


if __name__ == "__main__":
    # fire.Fire(AlignmentDataset)
    dataset = AlignmentDataset()
    blogs_to_fetch = ['aisafety.info']
    for blog in blogs_to_fetch:
        dataset.fetch(blog, rebuild=True)