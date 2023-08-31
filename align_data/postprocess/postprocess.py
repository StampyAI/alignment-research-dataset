# %%
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from typing import List, DefaultDict
import logging
from pathlib import Path

import jsonlines
from tqdm import tqdm
import pylab as plt
from nltk.tokenize import sent_tokenize, word_tokenize
import seaborn as sns #TODO: install seaborn or fix this file
import pandas as pd

logger = logging.getLogger(__name__)


#TODO: fix this file
@dataclass
class PostProcesser:
    """
    This class is used to postprocess the data
    """
    jsonl_path: Path = field(default_factory=lambda: (Path(__file__).parent / '../../data/').resolve())

    def __post_init__(self) -> None:
        print(f"Looking for data in {self.jsonl_path}")

        # Check if the directory exists
        if not self.jsonl_path.is_dir():
            raise FileNotFoundError(f"Data directory {self.jsonl_path} does not exist")

        self.jsonl_list: List[Path] = sorted(self.jsonl_path.glob("*.jsonl"))
        self.source_list: List[str] = [path.stem for path in self.jsonl_list]
        self.all_stats: DefaultDict[str, Counter] = defaultdict(Counter)

    def compute_statistics(self) -> None:
        for source_name, path in tqdm(zip(self.source_list, self.jsonl_list)):
            with jsonlines.open(path) as reader:
                for obj in reader:
                    text: str = obj['text']
                    source_stats = self.all_stats[source_name]
                    source_stats["num_entries"] += 1
                    source_stats["num_tokens"] += len(word_tokenize(text))
                    source_stats["num_chars"] += len(text)
                    source_stats["num_words"] += len(text.split())
                    source_stats["num_sentences"] += len(sent_tokenize(text))
                    source_stats["num_newlines"] += len(text.split("\n"))
                    source_stats["num_paragraphs"] += len(text.split("\n\n"))

    def plot_statistics(self) -> None:
        all_df = pd.DataFrame(self.all_stats).T

        fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(15, 15))
        metrics_to_plot = [
            "num_entries",
            "num_tokens",
            "num_words",
            "num_sentences",
            "num_paragraphs",
            "num_chars",
        ]

        for i, metric in enumerate(metrics_to_plot):
            ax = axes[i // 2, i % 2]
            sns.barplot(x=all_df.index, y=all_df[metric], ax=ax)
            ax.set_title(metric)
            ax.set_ylabel('')
            ax.tick_params(axis='x', rotation=45)
            # Uncomment the next line if you want to apply a log scale for better visualization.
            # ax.set_yscale("log")

        plt.tight_layout()
        plt.show()


    def merge_all_files(self, out_dir: str) -> str:
        raise NotImplementedError

    def deduplicate(self) -> None:
        for path in tqdm(self.jsonl_list):
            with jsonlines.open(path, "r") as reader:
                all_obj = {obj["id"]: obj for obj in reader}
            with jsonlines.open(path, "w") as writer:
                for obj in all_obj.values():
                    writer.write(obj)

    def clean_dataset(self, merged_dataset_path: str) -> str:
        raise NotImplementedError


pp = PostProcesser()
# %%
pp.source_list
# %%
pp.compute_statistics()
print(pp.all_stats)
pp.plot_statistics()
# %%
pp.deduplicate()
# %%
