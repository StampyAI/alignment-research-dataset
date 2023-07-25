from unittest.mock import patch, Mock

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from align_data.sources.articles.parsers import (
    google_doc, medium_blog, parse_grobid, get_content_type, extract_gdrive_contents
)


SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://www.tei-c.org/ns/1.0 https://raw.githubusercontent.com/kermitt2/grobid/master/grobid-home/schemas/xsd/Grobid.xsd"
 xmlns:xlink="http://www.w3.org/1999/xlink">
    <teiHeader xml:lang="en">
        <fileDesc>
            <titleStmt>
                <title level="a" type="main">The title!!</title>
            </titleStmt>
            <sourceDesc>
                <biblStruct>
                    <analytic>
                        <author>
                            <persName><forename type="first">Cullen</forename><surname>OâKeefe</surname></persName>
                        </author>
                    </analytic>
                </biblStruct>
            </sourceDesc>
        </fileDesc>
        <encodingDesc>
            <appInfo>
                <application version="0.7.0" ident="GROBID" when="2022-03-25T06:04+0000">
                    <desc>GROBID - A machine learning software for extracting information from scholarly documents</desc>
                    <ref target="https://github.com/kermitt2/grobid"/>
                </application>
            </appInfo>
        </encodingDesc>
        <profileDesc>
            <abstract>this is the abstract</abstract>
       </profileDesc>
    </teiHeader>
    <text xml:lang="en">
        <body>This is the contents</body>
    </text>
</TEI>
"""

def test_google_doc():
    def fetcher(url, *args, **kwargs):
        assert url == 'https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/export?format=html'
        return Mock(content="""
        <html>
          <header>bla bla bla</header>
          <body>
             ble ble <a href="bla.com">a link</a>

          </body>
        </html>
        """)

    with patch('requests.get', fetcher):
        assert google_doc('https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/edit') == 'ble ble [a link](bla.com)'


def test_google_doc_no_body():
    def fetcher(url, *args, **kwargs):
        assert url == 'https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/export?format=html'
        return Mock(content="<html> <header>bla bla bla</header> </html>")

    with patch('requests.get', fetcher):
        assert google_doc('https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/edit') is None


def test_google_doc_bad_url():
    assert google_doc('https://docs.google.com/bla/bla') is None


def test_medium_blog():
    html = """
    <article>
      <div>
        <h1>This is the title</h1>
        <div>
          <span>Some random thing</span>
          <span>another random thing</span>
          <span>Oct 7, 2023</span>
          <span>more random stuff</span>
         </div>
       </div>
       <div>
         bla bla bla <a href="http://ble.com">a link</a> bla bla
       </div>
    </article>
    """
    with patch('requests.get', return_value=Mock(content=html)):
        assert medium_blog('bla.com') == "bla bla bla [a link](http://ble.com) bla bla"


def test_medium_blog_no_title():
    html = """
    <article>
      <div>
          <span>Some random thing</span>
       </div>
       <div>
         bla bla bla <a href="http://ble.com">a link</a> bla bla
       </div>
    </article>
    """
    with patch('requests.get', return_value=Mock(content=html)):
        assert medium_blog('bla.com') == "Some random thing\n\n\n bla bla bla [a link](http://ble.com) bla bla"


def test_medium_blog_no_contents():
    html = """
    <div>
      <div>
          <span>Some random thing</span>
       </div>
       <div>
         bla bla bla <a href="http://ble.com">a link</a> bla bla
       </div>
    </div>
    """
    with patch('requests.get', return_value=Mock(content=html)):
        assert medium_blog('bla.com') is None


def test_parse_grobid():
    assert parse_grobid(SAMPLE_XML) == {
        'abstract': 'this is the abstract',
        'authors': ['Cullen Oâ\x80\x99Keefe'],
        'text': 'This is the contents',
        'title': 'The title!!',
        'data_source': 'xml',
    }


def test_parse_grobid_no_body():
    xml = """<?xml version="1.0" encoding="UTF-8"?>
        <TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.tei-c.org/ns/1.0 https://raw.githubusercontent.com/kermitt2/grobid/master/grobid-home/schemas/xsd/Grobid.xsd"
        xmlns:xlink="http://www.w3.org/1999/xlink">
            <teiHeader xml:lang="en">
            <encodingDesc>
               <appInfo>
                 <application version="0.7.0" ident="GROBID" when="2022-03-25T06:04+0000">
                 </application>
                </appInfo>
            </encodingDesc>
            </teiHeader>
            <text xml:lang="en">
            </text>
        </TEI>
    """
    assert parse_grobid(xml) == {'error': 'No contents in XML file', 'data_source': 'xml'}


@pytest.mark.parametrize('header, expected', (
    (None, set()),
    ('', set()),

    ('text/html', {'text/html'}),
    ('text/html; bla=asdas; fewwe=fe', {'text/html', 'bla=asdas', 'fewwe=fe'}),
))
def test_get_content_type(header, expected):
    assert get_content_type(Mock(headers={'Content-Type': header})) == expected


@pytest.mark.parametrize('headers', (
    {},
    {'Content-Type': None},
    {'Content-Type': ''},
    {'Content-Type': '     '},
    {'Content-Type': '  ; ;;   '},
))
def test_extract_gdrive_contents_no_contents(headers):
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'
    with patch('requests.head', return_value=Mock(headers=headers, status_code=200)):
        assert extract_gdrive_contents(url) == {
            'downloaded_from': 'google drive',
            'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
            'error': 'no content type'
        }


@pytest.mark.parametrize('header', (
    'application/octet-stream',
    'application/pdf',
    'application/pdf; filename=bla.pdf'
))
def test_extract_gdrive_contents_pdf(header):
    res = Mock(headers={'Content-Type': header}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'
    with patch('requests.head', return_value=res):
        with patch('align_data.articles.parsers.fetch_pdf', return_value={'text': 'bla'}):
            assert extract_gdrive_contents(url) == {
                'downloaded_from': 'google drive',
                'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
                'text': 'bla',
            }


@pytest.mark.parametrize('header', (
    'application/epub',
    'application/epub+zip',
    'application/epub; filename=bla.epub',
))
def test_extract_gdrive_contents_ebook(header):
    res = Mock(headers={'Content-Type': header}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'
    with patch('requests.head', return_value=res):
        assert extract_gdrive_contents(url) == {
            'downloaded_from': 'google drive',
            'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
            'data_source': 'ebook',
        }


def test_extract_gdrive_contents_html():
    res = Mock(headers={'Content-Type': 'text/html'}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'
    with patch('requests.head', return_value=Mock(headers={'Content-Type': 'text/html'}, status_code=200)):
        html = """
            <html>
               <header>bleee</header>
               <body>bla bla</body>
            </html>
        """
        res = Mock(
            headers={'Content-Type': 'text/html'},
            status_code=200,
            content=html,
            text=html,
        )
        with patch('requests.get', return_value=res):
            assert extract_gdrive_contents(url) == {
                'downloaded_from': 'google drive',
                'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
                'text': 'bla bla',
                'data_source': 'html',
            }


def test_extract_gdrive_contents_xml():
    res = Mock(headers={'Content-Type': 'text/html'}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'
    with patch('requests.head', return_value=Mock(headers={'Content-Type': 'text/html'}, status_code=200)):
        res = Mock(
            headers={'Content-Type': 'text/xml'},
            status_code=200,
            content=SAMPLE_XML,
            text=SAMPLE_XML,
        )
        with patch('requests.get', return_value=res):
            assert extract_gdrive_contents(url) == {
                'abstract': 'this is the abstract',
                'authors': ['Cullen Oâ\x80\x99Keefe'],
                'downloaded_from': 'google drive',
                'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
                'text': 'This is the contents',
                'title': 'The title!!',
                'data_source': 'xml',
            }


def test_extract_gdrive_contents_xml_with_confirm():
    res = Mock(headers={'Content-Type': 'text/html'}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'

    def fetcher(link, *args, **kwargs):
        # The first request should get the google drive warning page
        if link != "fetch/xml/contents":
            html = """
                   <body>
                      <title>Google Drive - Virus scan warning</title>
                      <form action="fetch/xml/contents"></form>
                   </body>
                """
            return Mock(headers={'Content-Type': 'text/html'}, status_code=200, text=html, content=html)

        # The second one returns the actual contents
        return Mock(headers={'Content-Type': 'text/xml'}, status_code=200, content=SAMPLE_XML)

    with patch('requests.head', return_value=Mock(headers={'Content-Type': 'text/html'}, status_code=200)):
        with patch('requests.get', fetcher):
            assert extract_gdrive_contents(url) == {
                'abstract': 'this is the abstract',
                'authors': ['Cullen Oâ\x80\x99Keefe'],
                'downloaded_from': 'google drive',
                'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
                'text': 'This is the contents',
                'title': 'The title!!',
                'data_source': 'xml',
            }


def test_extract_gdrive_contents_warning_with_unknown():
    res = Mock(headers={'Content-Type': 'text/html'}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'

    def fetcher(link, *args, **kwargs):
        # The first request should get the google drive warning page
        if link != "fetch/xml/contents":
            html = """
                   <body>
                      <title>Google Drive - Virus scan warning</title>
                      <form action="fetch/xml/contents"></form>
                   </body>
                """
            return Mock(headers={'Content-Type': 'text/html'}, status_code=200, text=html, content=html)

        # The second one returns the actual contents, with an unhandled content type
        return Mock(headers={'Content-Type': 'text/bla bla'}, status_code=200)

    with patch('requests.head', return_value=Mock(headers={'Content-Type': 'text/html'}, status_code=200)):
        with patch('requests.get', fetcher):
            assert extract_gdrive_contents(url) == {
                'downloaded_from': 'google drive',
                'error': "unknown content type: {'text/bla bla'}",
                'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
            }


def test_extract_gdrive_contents_unknown_content_type():
    res = Mock(headers={'Content-Type': 'bla bla'}, status_code=200)
    url = 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing'
    with patch('requests.head', return_value=res):
        assert extract_gdrive_contents(url) == {
            'downloaded_from': 'google drive',
            'source_url': 'https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing',
            'error': "unknown content type: {'bla bla'}",
        }
