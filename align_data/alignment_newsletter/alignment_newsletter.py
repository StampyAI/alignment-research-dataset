# %%
import logging
import jsonlines

import pandas as pd

from dataclasses import dataclass
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

logger = logging.getLogger(__name__)


@dataclass
class AlignmentNewsletter(AlignmentDataset):

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
        if not row.Summary or pd.isna(row.Summary):
            return None

        def handle_na(v, cast=None):
            if not v or pd.isna(v):
                return None
            if cast:
                return cast(v)
            return v

        return DataEntry({
            "url": handle_na(row.URL) or 'n/a',
            "source": handle_na(self.name),
            "converted_with": "python",
            "source_type": "google-sheets",
            "venue": handle_na(row.Venue, str),  # arXiv, Distill, LessWrong, Alignment Forum, ICML 2018, etc
            "newsletter_category": handle_na(row.Category, str),
            "highlight": row[2] == "Highlight",
            "newsletter_number": handle_na(row.Email, str),
            "summarizer": handle_na(row.Summarizer, str),
            "opinion": handle_na(row[11], str),
            "prerequisites": handle_na(row.Prerequisites, str),
            "read_more": handle_na(row[13], str),
            "title": handle_na(row.Title, str),
            "authors": [i.strip() for i in str(row.Authors).split(',')],
            "date_published": str(handle_na(row.Year, int) or 'n/a'),
            "text": handle_na(row.Summary, str),
        })
