from align_data.sources.articles.datasets import (
    ArxivPapers,
    EbookArticles,
    DocArticles,
    HTMLArticles,
    MarkdownArticles,
    PDFArticles,
    SpecialDocs,
    XMLArticles,
)
from align_data.sources.articles.indices import IndicesDataset
from align_data.common.alignment_dataset import MultiDataset


ARTICLES_DATASETS = [
    PDFArticles(
        name="pdfs",
        spreadsheet_id="1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4",
        sheet_id="0",
    ),
    HTMLArticles(
        name="html_articles",
        spreadsheet_id="1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4",
        sheet_id="759210636",
    ),
    EbookArticles(
        name="ebooks",
        spreadsheet_id="1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4",
        sheet_id="1800487220",
    ),
    XMLArticles(
        name="xmls",
        spreadsheet_id="1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4",
        sheet_id="823056509",
    ),
    MarkdownArticles(
        name="markdown",
        spreadsheet_id="1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4",
        sheet_id="1003473759",
    ),
    DocArticles(
        name="gdocs",
        spreadsheet_id="1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4",
        sheet_id="1293295703",
    ),
    SpecialDocs(
        "special_docs",
        spreadsheet_id="1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI",
        sheet_id="980957638",
    ),
]


ARTICLES_REGISTRY = [
    MultiDataset(name="special_docs", datasets=ARTICLES_DATASETS),
    ArxivPapers(
        name="arxiv",
        spreadsheet_id="1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI",
        sheet_id="655836697",
    ),
    IndicesDataset("indices"),
]
