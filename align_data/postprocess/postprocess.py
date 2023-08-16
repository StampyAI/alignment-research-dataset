# %%
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging
import jsonlines
from tqdm import tqdm
from pathlib import Path
import pylab as plt
# import seaborn as sns #TODO: install seaborn or fix this file
import pandas as pd

import logging
logger = logging.getLogger(__name__)



@dataclass
class PostProcesser:
    """
    This class is used to postprocess the data
    """
    jsonl_path: Path = field(default_factory=lambda: (Path(__file__).parent / '../../data/').resolve())

    def __post_init__(self) -> None:
        # Resolve the path to an absolute path (this doesn't guarantee the directory exists)
        print(f"Looking for data in {self.jsonl_path}")

        # Check if the directory exists
        if not self.jsonl_path.is_dir():
            raise FileNotFoundError(f"Data directory {self.jsonl_path} does not exist")

        self.jsonl_list: List[Path] = sorted(self.jsonl_path.glob("*.jsonl"))
        self.source_list: List[str] = [path.stem for path in self.jsonl_list]
        self.all_stats = defaultdict(Counter)

    def compute_statistics(self) -> None:
        for source_name, path in tqdm(zip(self.source_list, self.jsonl_list)):
            with jsonlines.open(path) as reader:
                for obj in reader:
                    text = obj['text']
                    source_stats = self.all_stats[source_name]
                    source_stats["num_entries"] += 1
                    source_stats["num_tokens"] += len(text.split())  # TODO: Use tokenizer
                    source_stats["num_chars"] += len(text)
                    source_stats["num_words"] += len(text.split())
                    source_stats["num_sentences"] += len(text.split("."))  # TODO: Use NLTK/Spacy or similar
                    source_stats["num_paragraphs"] += len(text.splitlines())

    # def plot_statistics(self) -> None: #TODO: fix this function
    #     all_df = pd.DataFrame(self.all_stats).T
    #     plt.figure(figsize=(5, 5))
    #     sns.barplot(x=all_df.index, y=all_df["num_entries"])

    def merge_all_files(self, out_dir: str) -> str:
        pass

    def deduplicate(self) -> None:
        for path in tqdm(self.jsonl_list):
            with jsonlines.open(path, "r") as reader:
                all_obj = {obj["id"]: obj for obj in reader}
            with jsonlines.open(path, "w") as writer:
                for obj in all_obj.values():
                    writer.write(obj)

    def clean_dataset(self, merged_dataset_path: str) -> str:
        pass


pp = PostProcesser()
# %%
pp.source_list
# %%
pp.compute_statistics()
# %%
pp.deduplicate()
# %%
