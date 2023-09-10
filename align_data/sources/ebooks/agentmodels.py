from pathlib import Path
from dataclasses import dataclass
import logging
from datetime import datetime, timezone

from align_data.common.alignment_dataset import AlignmentDataset
from git import Repo

logger = logging.getLogger(__name__)


@dataclass
class AgentModels(AlignmentDataset):
    """
    Grabs the "Modeling Agents with Probabilistic Programs" by Owain Evans, Andreas Stuhlmüller,
    John Salvatier, and Daniel Filan as .md from GitHub
    """

    repo: str = "https://github.com/agentmodels/agentmodels.org.git"
    done_key = "filename"

    def setup(self):
        super().setup()
        self.base_dir = self.raw_data_path / "agentmodels.org"
        if not self.base_dir.exists() or not list(self.base_dir.glob("*")):
            logger.info("Cloning repo")
            Repo.clone_from(url=self.repo, to_path=self.base_dir)
        self.repository = Repo(self.base_dir)
        self.files_path = self.base_dir / "chapters"

    @property
    def items_list(self):
        return self.files_path.iterdir()

    def _get_published_date(self, filepath: Path) -> datetime:
        last_commit = next(self.repository.iter_commits(paths=f"chapters/{filepath.name}"))
        return last_commit.committed_datetime.astimezone(timezone.utc)
    
    def _get_title(self, filepath: Path) -> str | None:
        """
        Receives a filepath, and retrieves the title.
        Examples:
            if filepath.stem: 6-efficient-inference
            then title: Modeling Agents with Probabilistic Programs - Chapter 6: Efficient Inference"

            if filepath.stem: 2-webppl
            then title: Modeling Agents with Probabilistic Programs - Chapter 2: Webppl"
        """
        if filepath.stem[:1].isnumeric():
            chapter_num, chapter_name = filepath.stem.split("-", 1)
            chapter_name = chapter_name.replace('-', ' ').capitalize()
            return f"Modeling Agents with Probabilistic Programs - Chapter {chapter_num}: {chapter_name}"
        chapter_name = filepath.stem.replace('-', ' ').capitalize()
        return f"Modeling Agents with Probabilistic Programs - {chapter_name}"

    def _get_url(self, filepath: Path) -> str | None:
        """
        Receives a filepath and retrieves the url.
        Examples:
            if filepath.stem: 6-efficient-inference
            then url: https://agentmodels.org/chapters/6-efficient-inference.html"

            if filepath.stem: .3d-something
            then url: None
        """
        if filepath.stem.startswith('.'):
            return None # unusual file
        #TODO: The website has "hidden" the pages for chapter 6 (filepath.stem.startswith("6")), so the
        # link doesn't point to the actual text of this chapter. To fix.
        return f"https://agentmodels.org/chapters/{filepath.stem}.html"

    def process_entry(self, filepath):
        return self.make_data_entry(
            {
                "source": self.name,
                "source_type": "markdown",
                "authors": [
                    "Owain Evans",
                    "Andreas Stuhlmüller",
                    "John Salvatier",
                    "Daniel Filan",
                ],
                "date_published": self._get_published_date(filepath),
                "title": self._get_title(filepath),
                "url": self._get_url(filepath),
                "filename": filepath.name,
                "text": filepath.read_text(encoding="utf-8"),
            }
        )
