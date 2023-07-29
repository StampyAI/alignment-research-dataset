from align_data.sources.articles.datasets import (
    PDFArticles, HTMLArticles, EbookArticles, XMLArticles, MarkdownArticles
)

ARTICLES_REGISTRY = [
    PDFArticles(
        name='pdfs',
        spreadsheet_id='1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4',
        sheet_id='0'
    ),
    HTMLArticles(
        name='html_articles',
        spreadsheet_id='1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4',
        sheet_id='759210636'
    ),
    EbookArticles(
        name='ebooks',
        spreadsheet_id='1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4',
        sheet_id='1800487220'
    ),
    XMLArticles(
        name='nonarxiv_papers',
        spreadsheet_id='1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4',
        sheet_id='823056509'
    ),
    MarkdownArticles(
        name='markdown',
        spreadsheet_id='1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4',
        sheet_id='1003473759'
    ),
]
