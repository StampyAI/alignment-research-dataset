from dataclasses import dataclass
import logging
from align_data.common.alignment_dataset import GdocDataset, DataEntry
import grobid_tei_xml

logger = logging.getLogger(__name__)

@dataclass
class NonarxivPapers(GdocDataset):

    summary_key = 'summary'
    done_key = "filename"
    glob = "*.xml"

    def setup(self):
        super().setup()

        self.files_path = self.raw_data_path / 'nonarxiv_teis'
        if not self.files_path.exists():
            self.zip_from_gdrive(path=self.raw_data_path)

    @property
    def zip_file(self):
        return self.raw_data_path / "nonarxiv_teis.zip"

    def process_entry(self, filename):
        logger.info(f"Processing {filename.name}")
        try:
            xml_text = filename.read_text(encoding='utf-8')
            doc_dict = grobid_tei_xml.parse_document_xml(xml_text).to_dict()
            authors = [xx["full_name"].strip(' !') for xx in doc_dict["header"]["authors"]]

            logger.info(f"Doc: {list(doc_dict.keys())}")

            return DataEntry({
                "title": doc_dict["header"]["title"],
                "abstract": doc_dict["abstract"],
                "text": doc_dict["body"],
                "date_published": "n/a",
                "url": "n/a",
                "authors": list(filter(None, authors)),
                "source": self.name,
                "source_type": "pdf",
                "filename": filename.name,
            })
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.info('Skipping %s', filename.name)

        return None
