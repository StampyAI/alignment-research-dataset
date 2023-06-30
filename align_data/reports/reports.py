from dataclasses import dataclass
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import logging
import grobid_tei_xml

from datetime import datetime, timezone
from dateutil.parser import parse

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
    
    @staticmethod
    def _get_published_data(doc_dict):
        date_str = doc_dict["header"].get('date')
        if date_str:
            dt = parse(date_str).astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return 'n/a'

    def process_entry(self, filename):
        logger.info(f"Processing {filename.name}")
        xml_text = filename.read_text(encoding='utf-8')
        try:
            doc_dict = grobid_tei_xml.parse_document_xml(xml_text).to_dict()
            abstract = doc_dict.get("abstract")
            logger.info(f"Doc: {list(doc_dict.keys())}")
            return DataEntry({
                "summary": [abstract] if abstract else [],
                "authors": [xx["full_name"] for xx in doc_dict["header"]["authors"]],
                "title": doc_dict["header"]["title"],
                "text": doc_dict["body"],
                "source": self.name,
                "source_type": "pdf",
                "date_published": self._get_published_data(doc_dict),
                "url": "n/a",
                "filename": filename.name,
            })
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.info('Skipping %s', filename.name)

        return None
