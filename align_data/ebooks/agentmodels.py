from align_data.common.alignment_dataset import AlignmentDataset, DataEntry
from dataclasses import dataclass
from git import Repo
import logging
from datetime import timezone

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
        self.base_dir = self.raw_data_path / 'agentmodels.org'
        if not self.base_dir.exists() or not list(self.base_dir.glob('*')):
            logger.info("Cloning repo")
            Repo.clone_from(self.repo, self.base_dir)
        self.repository = Repo(self.base_dir)
        self.files_path = self.base_dir / 'chapters'

    def _get_published_date(self, filename):
        last_commit = next(self.repository.iter_commits(paths=f'chapters/{filename.name}'))
        dt = last_commit.committed_datetime.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def process_entry(self, filename):
        return DataEntry({
            'source': self.name,
            'source_type': 'markdown',
            'authors': ['Owain Evans', 'Andreas Stuhlmüller', 'John Salvatier', 'Daniel Filan'],
            'date_published': self._get_published_date(filename),
            'title': 'Modeling Agents with Probabilistic Programs',
            'url': f'https://agentmodels.org/chapters/{filename.stem}.html', 
            'text': filename.read_text(encoding='utf-8'),
        })