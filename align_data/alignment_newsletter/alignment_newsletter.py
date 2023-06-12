# %%
import logging
import jsonlines

import pandas as pd

from dataclasses import dataclass
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry
from tqdm import tqdm

logger = logging.getLogger(__name__)


@dataclass
class AlignmentNewsletter(AlignmentDataset):

    COOLDOWN: int = 1
    done_key = "title"

    def setup(self) -> None:
        super().setup()
        self.newsletter_xlsx_path = self.raw_data_path / "alignment_newsletter.xlsx"
        self.df = pd.read_excel(self.newsletter_xlsx_path)

    def get_item_key(self, row):
        return row.Title

    @property
    def items_list(self):
        return self.df.itertuples()

    def process_entry(self, row):
        """
        For each row in the dataframe, create a new entry with the following fields: url, source,
        converted_with, source_type, venue, newsletter_category, highlight, newsletter_number,
        summarizer, opinion, prerequisites, read_more, title, authors, date_published, text
        """
        return DataEntry({
            "url": "https://rohinshah.com/alignment-newsletter/",
            "source": "alignment newsletter",
            "converted_with": "python",
            "source_type": "google-sheets",
            "venue": str(row.Venue),  # arXiv, Distill, LessWrong, Alignment Forum, ICML 2018, etc
            "newsletter_category": str(row.Category),
            "highlight": row[2] == "Highlight",
            "newsletter_number": str(row.Email),
            "summarizer": str(row.Summarizer),
            "opinion": str(row[11]),
            "prerequisites": str(row.Prerequisites),
            "read_more": str(row[13]),
            "title": str(row.Title),
            "authors": str(row.Authors),
            "date_published": row.Year,
            "text": str(row.Summary),
        })
