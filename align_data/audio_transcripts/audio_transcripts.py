from dataclasses import dataclass
from align_data.common.alignment_dataset import GdocDataset
import logging
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

@dataclass
class AudioTranscripts(GdocDataset):

    done_key = 'filename'

    def setup(self):
        super().setup()

        self.files_path = self.raw_data_path / 'transcripts'
        if not self.files_path.exists():
            self.files_path.mkdir(parents=True, exist_ok=True)
            self.zip_from_gdrive(path=self.raw_data_path)

    @staticmethod
    def extract_authors(text):
        """Attempt to extract the authors from the text.

        The first line tends to be the title, which tends to contain info about who's talking,
        so do some black magic to try to guess at the names
        """
        firstline = text.split('\n')[0].strip('# ')
        # e.g. 'Interview with AI Researchers individuallyselected_84py7 by Vael Gates'
        if firstline.startswith('Interview with '):
            return firstline.split(' by ')[1:]
        # e.g. 'Alex Turner on Will Advanced AIs Tend To Seek Power by Jeremie Harris on the  Towards Data Science Podcast'
        if ' by Jeremie Harris on the Towards Data Science Podcast' in firstline:
            person = firstline.split(' on ')[0]
            return [person, 'Jeremie Harris']
        # e.g. 'Markus Anderljung and Ben Garfinkel Fireside chat on AI governance - EA Forum'
        if re.search('[^)] - EA Forum$', firstline):
            return re.findall("(?:^|(?:and ))([A-Z]\w+ (?:\w+')?[A-Z]\w+)", firstline)
        # e.g. 'The AI revolution and international politics (Allan Dafoe) - EA Forum'
        if res := re.search('\((.*?)\) - EA Forum$', firstline):
            return [res.group(1)]
        # e.g. 'Iason Gabriel on Foundational Philosophical Questions in AI Alignment - Future of Life Institute'
        if re.search('^([A-Z]\w+ )+[oO]n', firstline):
            return [re.search('^(.*?) [oO]n', firstline).group(1)]
        # e.g. 'AGI Safety and Alignment with Robert Miles on the Machine Ethics Podcast'
        if res := re.search(' with (.*?) [oO]n', firstline):
            return [res.group((1))]
        # e.g. 'Rohin Shah: What\xe2\x80\x99s been happening in AI alignment?'
        if res := re.search('^(.*?):', firstline):
            return [res.group(1)]
        return []
    
    @staticmethod
    def _get_published_date(filename):
        date_str = re.search(r"\d{4}\d{2}\d{2}", str(filename))
        if not date_str:
            return ''
        date_str = date_str.group(0)
        dt = datetime.strptime(date_str, "%Y%m%d").astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


    def process_entry(self, filename):
        logger.info(f"Processing {filename.name}")
        text = filename.read_text(encoding="utf-8")
        title = filename.stem

        return self.make_data_entry({
            "source": self.name,
            "source_type": "audio",
            "url": "",
            "converted_with": "otter-ai",
            "title": title,
            "authors": self.extract_authors(text),
            "date_published": self._get_published_date(filename),
            "text": text,
            'filename': filename.name,
        })
