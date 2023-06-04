from dataclasses import dataclass
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
import grobid_tei_xml
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class Reports(GdocDataset):

    done_key = "filename"

    def setup(self):
        self._setup()

        self.glob = "*.xml"
        self.files_path = self.raw_data_path / 'report_teis'
        self.zip_from_gdrive(path=self.raw_data_path)

    @property
    def zip_file(self):
        return self.raw_data_path / "report_teis.zip"

    def fetch_entries(self):
        self.setup()
        for ii, filename in enumerate(tqdm(self.file_list)):
            if self._entry_done(filename.name):
                # logger.info(f"Already done {filename}")
                continue

            logger.info(f"Processing {filename}")
            data = {
                "source": self.name,
                "source_filetype": "pdf",
                "authors": "n/a",
                "title": "n/a",
                "text": "n/a",
                "date_published": "n/a",
                "url": "n/a",
                "filename": filename.name,
            }

            xml_text = filename.read_text()
            try:
                doc_dict = grobid_tei_xml.parse_document_xml(xml_text).to_dict()

                logger.info(f"Doc: {list(doc_dict.keys())}")
                data["abstract"] = doc_dict["abstract"]
                data["authors"] = [xx["full_name"] for xx in doc_dict["header"]["authors"]]
                data["title"] = doc_dict["header"]["title"]
                data["text"] = doc_dict["body"]
            except Exception as e:
                logger.error(f"Error: {e}")

            new_entry = DataEntry(data)
            new_entry.add_id()
            yield new_entry
