from .gdocs import Gdocs
from .gsheets import GSheets

# 2022-06-01: Current iteration only include "AI Researcher Interviews"
# from https://www.lesswrong.com/posts/LfHWhcfK92qh2nwku/transcripts-of-interviews-with-ai-researchers

GDOCS_REGISTRY = [
    Gdocs(name="gdocs", gdrive_address="https://drive.google.com/uc?id=18uFLj3Vs8de6LnEE00taJAvPl8dZYRxx"),
    GSheets(
        name='special_docs',
        sheet_id='980957638',
        spreadsheet_id='1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI',
        mappings = {
            'summary': ['Abstract Note', 'Rohin_Shah_Blurb'],
        },
        extra_fields = ['ISBN', 'ISSN', 'DOI'],
    )
]
