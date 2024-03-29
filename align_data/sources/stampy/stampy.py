import sys
import re
import logging
from dataclasses import dataclass

from codaio import Coda, Document
import html

from align_data.common.alignment_dataset import AlignmentDataset
from align_data.settings import CODA_TOKEN, CODA_DOC_ID, ON_SITE_TABLE

logger = logging.getLogger(__name__)


@dataclass
class Stampy(AlignmentDataset):
    done_key = "title"

    def setup(self):
        if not CODA_TOKEN:
            print(
                f"No CODA_TOKEN found! Please provide a valid Read token for the {CODA_DOC_ID} table"
            )
            sys.exit(1)

        super().setup()

    @property
    def items_list(self):
        coda = Coda(CODA_TOKEN)
        doc = Document(CODA_DOC_ID, coda=coda)
        logger.info("Fetching table: %s", CODA_DOC_ID)
        table = doc.get_table(ON_SITE_TABLE)
        return table.to_dict()  # a list of dicts

    def get_item_key(self, entry) -> str:
        return html.unescape(entry["Question"])

    def _get_published_date(self, entry):
        date_published = entry["Doc Last Edited"]
        return super()._get_published_date(date_published)

    def process_entry(self, entry):
        def clean_text(text):
            text = html.unescape(text)
            return re.sub(r"\(/\?state=(\w+)\)", r"(http://aisafety.info?state=\1)", text)

        question = clean_text(entry["Question"])  # raise an error if the entry has no question
        answer = clean_text(entry["Rich Text"])
        url = "https://aisafety.info?state=" + entry["UI ID"]

        logger.info(f"Processing {question}")

        return self.make_data_entry(
            {
                "source": self.name,
                "source_type": "markdown",
                "url": url,
                "title": question,
                "authors": ["Stampy aisafety.info"],
                "date_published": self._get_published_date(entry),
                "text": answer,
            }
        )
