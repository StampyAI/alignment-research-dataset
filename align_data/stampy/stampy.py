from dataclasses import dataclass
import requests
import logging
from align_data.common.alignment_dataset import AlignmentDataset , DataEntry
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class Stampy(AlignmentDataset):

    index_url : str
    done_key = "entry"

    @property
    def items_list(self):
        entries = dict(requests.get(self.index_url).json())
        return entries["results"].keys()

    def get_item_key(self, item):
        return item

    def process_entry(self, entry):
        qa_entry = entries["results"][entry]
        qa_entry["question"] = ' '.join(entry.split("to ")[1:])
        qa_entry["answer"] = entries["results"][entry]["printouts"]["Answer"]
        qa_entry["text"] = "Question: " + qa_entry["question"] + "\n\nAnswer: " + entries["results"][entry]["printouts"]["Answer"][0]
        # if there is more than one answer, add the rest
        for jj in range(1, len(entries["results"][entry]["printouts"]["Answer"])):
            qa_entry["text"] += f"\n\nAnswer {str(jj)}: " + entries["results"][entry]["printouts"]["Answer"][jj]


        logger.info(f"Processing {ii}")

        return DataEntry({
            "source" : self.name,
            "source_filetype": "text",
            "url": "n/a",
            "title": qa_entry["question"],
            "authors": "n/a",
            "date_published": "n/a",
            "text": qa_entry["text"],
            "question": qa_entry["question"],
            "answer": qa_entry["answer"],
            "entry": entry
        })
