import logging
from dataclasses import dataclass
from codaio import Coda, Document
from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

from align_data.stampy.settings import CODA_TOKEN, CODA_DOC_ID, ON_SITE_TABLE

logger = logging.getLogger(__name__)


@dataclass
class Stampy(AlignmentDataset):

    done_key = "question"

    @property
    def items_list(self):
        coda = Coda(CODA_TOKEN)
        doc = Document(CODA_DOC_ID, coda=coda)
        logger.info('Fetching table: %s', CODA_DOC_ID)
        table = doc.get_table(ON_SITE_TABLE)
        return table.to_dict() # a list of dicts

    def get_item_key(self, entry):
        return entry['Question']

    def process_entry(self, entry):
        question = entry['Question'] # raise an error if the entry has no question
        answer = entry['Rich Text']
        url = url = 'https://aisafety.info?state=' + entry['UI ID']
        date_published = entry['Doc Last Edited']

        logger.info(f"Processing {question}")

        return DataEntry({
            "source": self.name,
            "source_filetype": "text",
            "url": url,
            "title": question,
            "authors": "n/a",
            "date_published": date_published,
            "text": answer,
        })
