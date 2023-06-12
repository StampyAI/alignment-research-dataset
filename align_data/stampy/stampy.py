from dataclasses import dataclass
import requests
import logging
from align_data.common.alignment_dataset import AlignmentDataset , DataEntry
from tqdm import tqdm

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
    done_key = "entry"

    def setup(self):
        self._setup()

    def fetch_entries(self):
        self.setup()


        coda = Coda(CODA_TOKEN)
        doc = Document(self.coda_doc_id, coda=coda)
        table = doc.get_table(self.on_site_table)
        entries = table.to_dict() # a list of dicts
        #
        # keys for each element of the list of dicts: 
        # {'Edit Answer', 'Status', 'aisafety.info Link', 'External Source', 'Tags', 'Related IDs', 'Alternate Phrasings', 'Rich Text', 'Doc Last Edited', 'Question', 'Link', 'UI ID', 'Related Answers'}
        for ii, entry in enumerate(tqdm(entries)):
            if self._entry_done(entry.get('Question','')):
                # logger.info(f"Already done {entry}")
                continue

            question = entry.get('Question', '')
            answer = entry.get('Rich Text', '')  # Assuming 'Answer' is present in each dict
            url = entry.get('Link', '')
            date_published = entry.get('Doc Last Edited', '')

            # Creating the text field
            #text = f"Question: {question}\n\nAnswer: {answer}" note: we don't do this because the title contains the question already

            logger.info(f"Processing {ii}")

            new_entry = DataEntry({
                "source": self.name,
                "source_filetype": "text",
                "url": url,
                "title": question,
                "authors": "n/a",
                "date_published": date_published,
                "text": answer,
                "question": question,
                "answer": answer,
                #"entry": entry  # This is the entire dictionary entry
            })

            logger.info(f"Processing {new_entry}")
            new_entry.add_id()

            yield new_entry
