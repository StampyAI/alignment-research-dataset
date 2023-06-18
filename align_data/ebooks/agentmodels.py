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
        self.base_dir = self.raw_data_path / 'agentmodels.org'
        if not self.base_dir.exists():
            logger.info("Cloning repo")
            Repo.clone_from(self.repo, self.base_dir)
        self.repository = Repo(self.base_dir)
        self.files_path = self.base_dir / 'chapters'

    def _get_published_date(self, filename):
        try:
            last_commit = next(self.repository.iter_commits(paths=f'chapters/{filename.name}'))
            return last_commit.committed_datetime.isoformat()
        except Exception as e:
            logger.error(f'Error getting last modification date for {filename.name}: {e}')
        return "2016-01-08T10:50:56-8:00"  # date of the initial commit

    def process_entry(self, filename):
        return DataEntry({
            'source': self.name,
            'source_filetype': 'markdown',
            'converted_with': 'not converted',
            'book_title': 'Modeling Agents with Probabilistic Programs',
            'authors': ['Owain Evans', 'Andreas Stuhlmüller', 'John Salvatier', 'Daniel Filan'],
            'date_published': self._get_published_date(filename),
            'title': filename.name,
            'url': f'{self.repo[:-4]}/blob/gh-pages/chapters/{filename.name}',
            'text': filename.read_text(),
        })
