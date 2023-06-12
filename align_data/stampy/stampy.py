import os
import logging
from dataclasses import dataclass
import requests
from dotenv import load_dotenv
from codaio import Coda, Document
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

load_dotenv()
CODA_TOKEN = os.environ.get("CODA_TOKEN", "token not found")

import os
from dotenv import load_dotenv
load_dotenv()
CODA_TOKEN = os.environ.get("CODA_TOKEN", "token not found")
from codaio import Coda, Document

logger = logging.getLogger(__name__)


@dataclass
class Stampy(AlignmentDataset):

    coda_doc_id : str
    on_site_table : str
    done_key = "question"

    @property
    def items_list(self):
        coda = Coda(CODA_TOKEN)
        doc = Document(self.coda_doc_id, coda=coda)
        logger.info('Fetching table: %s', self.coda_doc_id)
        table = doc.get_table(self.on_site_table)
        return table.to_dict() # a list of dicts

    def get_item_key(self, entry):
        return entry.get('Question', '')

    def process_entry(self, entry):
        question = entry.get('Question', '')
        answer = entry.get('Rich Text', '')  # Assuming 'Answer' is present in each dict
        url = entry.get('Link', '')
        date_published = entry.get('Doc Last Edited', '')

        logger.info(f"Processing {question}")

        return DataEntry({
            "source": self.name,
            "source_filetype": "text",
            "url": url,
            "title": question,
            "authors": "n/a",
            "date_published": date_published,
            "text": answer,
            "question": question,
            "answer": answer,
        })
