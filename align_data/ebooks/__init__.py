from .agentmodels import AgentModels
from .gdrive_ebooks import GDrive
from .mdebooks import MDEBooks

EBOOK_REGISTRY = [
    AgentModels(
        name='agentmodels',
        repo='https://github.com/agentmodels/agentmodels.org.git'
    ),
    GDrive(
        name='gdrive_ebooks',
        gdrive_address=
        'https://drive.google.com/drive/folders/1V9-uVhUaxfWz5qw1sWLNRt0ikgSstc50'
    ),
    MDEBooks(
        name="markdown.ebooks",
        gdrive_address=
        'https://drive.google.com/uc?id=1diZwPT_HHAPFq-4RuiLx5poKsDu1oq1O'
    ),
]
