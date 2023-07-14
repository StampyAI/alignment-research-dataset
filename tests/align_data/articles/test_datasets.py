from unittest.mock import Mock, patch

import pandas as pd
import pytest
from align_data.articles.datasets import EbookArticles, HTMLArticles, PDFArticles, SpreadsheetDataset, XMLArticles


@pytest.fixture
def articles():
    source_type = 'something'
    articles = [
        {
            'source_url': f'http://example.com/source_url/{i}',
            'url': f'http://example.com/item/{i}',
            'title': f'article no {i}',
            'source_type': source_type,
            'date_published': f'2023/01/0{i + 1} 12:32:11',
            'authors': f'John Snow, mr Blobby',
            'summary': f'the summary of article {i}',
            'file_id': str(i),
        } for i in range(5)
    ]
    return pd.DataFrame(articles)


def test_spreadsheet_dataset_items_list(articles):
    dataset = SpreadsheetDataset(name='bla', spreadsheet_id='123', sheet_id='456')
    df = pd.concat(
        [articles, pd.DataFrame([{'title': None}, {'summary': 'bla'}])],
        ignore_index=True
    )
    with patch('pandas.read_csv', return_value=df):
        assert list(dataset.items_list) == list(pd.DataFrame(articles).itertuples())


def test_spreadsheet_dataset_get_item_key():
    dataset = SpreadsheetDataset(name='bla', spreadsheet_id='123', sheet_id='456')
    assert dataset.get_item_key(Mock(bla='ble', title='the key')) == 'the key'


@pytest.mark.parametrize('authors, expected', (
    ('', []),
    ('   \n \n  \t', []),
    ('John Snow', ['John Snow']),
    ('John Snow, mr. Blobby', ['John Snow', 'mr. Blobby']),
))
def test_spreadsheet_dataset_extract_authors(authors, expected):
    dataset = SpreadsheetDataset(name='bla', spreadsheet_id='123', sheet_id='456')
    assert dataset.extract_authors(Mock(authors=authors)) == expected


def test_pdf_articles_get_text():
    dataset = PDFArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    item = Mock(file_id='23423', title='bla bla bla')

    def check_downloads(filename, id):
        assert filename == str(dataset.files_path / 'bla bla bla.pdf')
        assert id == '23423'

    def read_pdf(filename):
        assert filename == dataset.files_path / 'bla bla bla.pdf'
        return 'pdf contents'

    with patch('align_data.articles.datasets.download', check_downloads):
        with patch('align_data.articles.datasets.read_pdf', read_pdf):
            assert dataset._get_text(item) == 'pdf contents'


def test_pdf_articles_process_item(articles):
    dataset = PDFArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    with patch('pandas.read_csv', return_value=articles):
        item = list(dataset.items_list)[0]

    with patch('align_data.articles.datasets.download'):
        with patch('align_data.articles.datasets.read_pdf', return_value='pdf contents <a href="asd.com">bla</a>'):
            assert dataset.process_entry(item).to_dict() == {
                'authors': ['John Snow', 'mr Blobby'],
                'date_published': '2023-01-01T12:32:11Z',
                'id': None,
                'source': 'bla',
                'source_filetype': 'pdf',
                'source_type': 'something',
                'summaries': ['the summary of article 0'],
                'text': 'pdf contents [bla](asd.com)',
                'title': 'article no 0',
                'url': 'http://example.com/item/0',
            }


def test_html_articles_get_text():
    def parser(url):
        assert url == 'http://example.org/bla.bla'
        return 'html contents'

    with patch('align_data.articles.datasets.HTML_PARSERS', {'example.org': parser}):
        assert HTMLArticles._get_text(Mock(source_url='http://example.org/bla.bla')) == 'html contents'


def test_html_articles_get_text_no_parser():
    with patch('align_data.articles.datasets.HTML_PARSERS', {}):
        assert HTMLArticles._get_text(Mock(source_url='http://example.org/bla.bla')) is None


def test_html_articles_process_entry(articles):
    dataset = HTMLArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    with patch('pandas.read_csv', return_value=articles):
        item = list(dataset.items_list)[0]

    parsers = {'example.com': lambda _: '   html contents with <a href="bla.com">proper elements</a> ble ble   '}
    with patch('align_data.articles.datasets.HTML_PARSERS', parsers):
        assert dataset.process_entry(item).to_dict() == {
            'authors': ['John Snow', 'mr Blobby'],
            'date_published': '2023-01-01T12:32:11Z',
            'id': None,
            'source': 'bla',
            'source_filetype': 'html',
            'source_type': 'something',
            'summaries': ['the summary of article 0'],
            'text': 'html contents with [proper elements](bla.com) ble ble',
            'title': 'article no 0',
            'url': 'http://example.com/item/0',
        }


def test_ebook_articles_get_text():
    dataset = EbookArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    item = Mock(
        source_url='https://drive.google.com/file/d/123456/view?usp=drive_link',
        title='bla bla bla'
    )

    def check_downloads(output, id):
        assert output == str(dataset.files_path / 'bla bla bla.epub')
        assert id == '123456'
        return output

    def read_ebook(filename, *args, **kwargs):
        return 'ebook contents'

    with patch('align_data.articles.datasets.download', check_downloads):
        with patch('pypandoc.convert_file', read_ebook):
            assert dataset._get_text(item) == 'ebook contents'


def test_ebook_articles_process_entry(articles):
    dataset = EbookArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    with patch('pandas.read_csv', return_value=articles):
        item = list(dataset.items_list)[0]

    contents = '   html contents with <a href="bla.com">proper elements</a> ble ble   '
    with patch('align_data.articles.datasets.download'):
        with patch('pypandoc.convert_file', return_value=contents):
            assert dataset.process_entry(item).to_dict() == {
                'authors': ['John Snow', 'mr Blobby'],
                'date_published': '2023-01-01T12:32:11Z',
                'id': None,
                'source': 'bla',
                'source_filetype': 'epub',
                'source_type': 'something',
                'summaries': ['the summary of article 0'],
                'text': 'html contents with [proper elements](bla.com) ble ble',
                'title': 'article no 0',
                'url': 'http://example.com/item/0',
            }


def test_xml_articles_get_text():
    dataset = XMLArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    with patch('align_data.articles.datasets.extract_gdrive_contents', return_value={'text': 'bla bla'}):
        assert dataset._get_text(Mock(source_url='bla.com')) == 'bla bla'


def test_xml_articles_process_entry(articles):
    dataset = XMLArticles(name='bla', spreadsheet_id='123', sheet_id='456')
    with patch('pandas.read_csv', return_value=articles):
        item = list(dataset.items_list)[0]

    with patch('align_data.articles.datasets.extract_gdrive_contents', return_value={'text': 'bla bla'}):
        assert dataset.process_entry(item).to_dict() == {
            'authors': ['John Snow', 'mr Blobby'],
            'date_published': '2023-01-01T12:32:11Z',
            'id': None,
            'source': 'bla',
            'source_filetype': 'xml',
            'source_type': 'something',
            'summaries': ['the summary of article 0'],
            'text': 'bla bla',
            'title': 'article no 0',
            'url': 'http://example.com/item/0',
        }
