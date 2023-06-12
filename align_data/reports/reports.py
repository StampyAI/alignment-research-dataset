from dataclasses import dataclass
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
import grobid_tei_xml
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class Reports(GdocDataset):

    done_key = "filename"
    glob = "*.xml"

    def setup(self):
        super().setup()

        logger.info('Fetching data from Gdrive')
        self.files_path = self.raw_data_path / 'report_teis'
        self.zip_from_gdrive(path=self.raw_data_path)
        logger.info('Fetched data')

    @property
    def zip_file(self):
        return self.raw_data_path / "report_teis.zip"

    def process_entry(self, filename):
        logger.info(f"Processing {filename.name}")
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

        return DataEntry(data)
