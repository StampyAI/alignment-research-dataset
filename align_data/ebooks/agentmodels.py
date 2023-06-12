from align_data.common.alignment_dataset import AlignmentDataset, DataEntry
from dataclasses import dataclass
from git import Repo
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

@dataclass
class AgentModels(AlignmentDataset):
    """
    Grabs the "Modeling Agents with Probabilistic Programs" by Owain Evans, Andreas Stuhlmüller,
    John Salvatier, and Daniel Filan as .md from GitHub
    """

    repo: str = 'https://github.com/agentmodels/agentmodels.org.git'
    done_key = "title"

    def setup(self):
        base_dir = self.raw_data_path / 'agentmodels.org'
        if not base_dir.exists():
            logger.info("Cloning repo")
            Repo.clone_from(self.repo, base_dir)
        self.files_path = base_dir / 'chapters'

    def process_entry(self, filename):
        return DataEntry({
            'source': 'agentmodels.org',
            'source_filetype': 'markdown',
            'converted_with': 'not converted',
            'book_title': 'Modeling Agents with Probabilistic Programs',
            'authors': ['Owain Evans', 'Andreas Stuhlmüller', 'John Salvatier', 'Daniel Filan'],
            'date_published': '2016',
            'title': filename.name,
            'url': self.repo,
            'text': filename.read_text(),
        })
