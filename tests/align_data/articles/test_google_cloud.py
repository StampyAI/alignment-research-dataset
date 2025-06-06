from unittest.mock import Mock, patch
from bs4 import BeautifulSoup

import pytest
from align_data.sources.articles.google_cloud import (
    extract_gdrive_contents,
    get_content_type,
    google_doc,
    parse_grobid,
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
    mock_response = Mock(
        content="""
        <html>
          <header>bla bla bla</header>
          <body>
             ble ble <a href="bla.com">a link</a>

          </body>
        </html>
        """
    )
    
    url = "https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/edit"
    
    with patch("align_data.sources.articles.google_cloud.fetch_element") as mock_fetch:
        mock_fetch.return_value = BeautifulSoup(mock_response.content, "html.parser").find("body")
        
        assert google_doc(url) == {
            "text": "ble ble [a link](bla.com)",
            "source_url": url,
        }
        
        # Verify the correct URL was called
        mock_fetch.assert_called_once_with(
            "https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/export?format=html",
            "body"
        )


def test_google_doc_no_body():
    url = "https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/edit"
    
    with patch("align_data.sources.articles.google_cloud.fetch_element") as mock_fetch:
        mock_fetch.return_value = None
        
        assert google_doc(url) == {}
        
        mock_fetch.assert_called_once_with(
            "https://docs.google.com/document/d/1fenKXrbvGeZ83hxYf_6mghsZMChxWXjGsZSqY3LZzms/export?format=html",
            "body"
        )


def test_google_doc_bad_url():
    assert google_doc("https://docs.google.com/bla/bla") == {}


def test_parse_grobid():
    assert parse_grobid(SAMPLE_XML) == {
        "abstract": "this is the abstract",
        "authors": ["Cullen Oâ\x80\x99Keefe"],
        "text": "This is the contents",
        "title": "The title!!",
        "source_type": "xml",
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
    assert parse_grobid(xml) == {
        "error": "No contents in XML file",
        "source_type": "xml",
    }


@pytest.mark.parametrize(
    "header, expected",
    (
        (None, set()),
        ("", set()),
        ("text/html", {"text/html"}),
        ("text/html; bla=asdas; fewwe=fe", {"text/html", "bla=asdas", "fewwe=fe"}),
    ),
)
def test_get_content_type(header, expected):
    assert get_content_type(Mock(headers={"Content-Type": header})) == expected


@pytest.mark.parametrize(
    "headers",
    (
        {},
        {"Content-Type": None},
        {"Content-Type": ""},
        {"Content-Type": "     "},
        {"Content-Type": "  ; ;;   "},
    ),
)
def test_extract_gdrive_contents_no_contents(headers):
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"
    with patch("align_data.sources.articles.google_cloud.fetch", return_value=Mock(headers=headers, status_code=200)) as mock_fetch:
        result = extract_gdrive_contents(url)
        print(f"Mock headers: {headers}")
        print(f"Result: {result}")
        assert result == {
            "downloaded_from": "google drive",
            "source_url": url,
            "error": "no content type",
        }


@pytest.mark.parametrize(
    "header",
    (
        "application/octet-stream",
        "application/pdf",
        "application/pdf; filename=bla.pdf",
    ),
)
def test_extract_gdrive_contents_pdf(header):
    res = Mock(headers={"Content-Type": header}, status_code=200)
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"
    with patch("align_data.sources.articles.google_cloud.fetch", return_value=res):
        with patch(
            "align_data.sources.articles.google_cloud.fetch_pdf",
            return_value={"text": "bla"},
        ):
            assert extract_gdrive_contents(url) == {
                "downloaded_from": "google drive",
                "source_url": url,
                "text": "bla",
            }


@pytest.mark.parametrize(
    "header",
    (
        "application/epub",
        "application/epub+zip",
        "application/epub; filename=bla.epub",
    ),
)
def test_extract_gdrive_contents_ebook(header):
    res = Mock(headers={"Content-Type": header}, status_code=200)
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"
    with patch("align_data.sources.articles.google_cloud.fetch", return_value=res):
        assert extract_gdrive_contents(url) == {
            "downloaded_from": "google drive",
            "source_url": "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing",
            "source_type": "ebook",
        }


def test_extract_gdrive_contents_html():
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"
    html = """
        <html>
           <header>bleee</header>
           <body>bla bla</body>
        </html>
    """
    
    mock_responses = [
        Mock(headers={"Content-Type": "text/html"}, status_code=200),  # First call response
        Mock(                                                          # Second call response
            headers={"Content-Type": "text/html"},
            status_code=200,
            content=html,
            text=html,
        )
    ]
    
    with patch("align_data.sources.articles.google_cloud.fetch", side_effect=mock_responses):
        assert extract_gdrive_contents(url) == {
            "downloaded_from": "google drive",
            "source_url": url,
            "text": "bla bla",
            "source_type": "html",
        }


def test_extract_gdrive_contents_xml():
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"
    
    mock_responses = [
        Mock(headers={"Content-Type": "text/html"}, status_code=200),  # First call response
        Mock(                                                          # Second call response
            headers={"Content-Type": "text/xml"},
            status_code=200,
            content=SAMPLE_XML,
            text=SAMPLE_XML,
        )
    ]

    with patch("align_data.sources.articles.google_cloud.fetch", side_effect=mock_responses):
        assert extract_gdrive_contents(url) == {
            "abstract": "this is the abstract",
            "authors": ["Cullen Oâ\x80\x99Keefe"],
            "downloaded_from": "google drive",
            "source_url": url,
            "text": "This is the contents",
            "title": "The title!!",
            "source_type": "xml",
        }


def test_extract_gdrive_contents_xml_with_confirm():
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"

    # Create the warning HTML page with a form that leads to the XML content
    warning_html = """
        <body>
            <title>Google Drive - Virus scan warning</title>
            <form action="fetch/xml/contents"><input type="hidden" name="id" value="foo" /></form>
        </body>
    """

    mock_responses = [
        Mock(headers={"Content-Type": "text/html"}, status_code=200),  # Initial check
        Mock(                                                          # Warning page
            headers={"Content-Type": "text/html"},
            status_code=200,
            content=warning_html,
            text=warning_html,
        ),
        Mock(                                                          # Final XML content
            headers={"Content-Type": "text/xml"},
            status_code=200,
            content=SAMPLE_XML,
        )
    ]

    with patch("align_data.sources.articles.google_cloud.fetch", side_effect=mock_responses):
        assert extract_gdrive_contents(url) == {
            "abstract": "this is the abstract",
            "authors": ["Cullen Oâ\x80\x99Keefe"],
            "downloaded_from": "google drive",
            "source_url": url,
            "text": "This is the contents",
            "title": "The title!!",
            "source_type": "xml",
        }


def test_extract_gdrive_contents_warning_with_unknown():
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"

    warning_html = """
        <body>
            <title>Google Drive - Virus scan warning</title>
            <form action="fetch/xml/contents"><input type="hidden" name="id" value="foo" /></form>
        </body>
    """

    # Set up three sequential responses:
    # 1. Initial content type check
    # 2. Warning page
    # 3. Unknown content type
    mock_responses = [
        Mock(headers={"Content-Type": "text/html"}, status_code=200),
        Mock(
            headers={"Content-Type": "text/html"},
            status_code=200,
            content=warning_html,
            text=warning_html,
        ),
        Mock(headers={"Content-Type": "text/bla bla"}, status_code=200)
    ]

    with patch("align_data.sources.articles.google_cloud.fetch", side_effect=mock_responses):
        assert extract_gdrive_contents(url) == {
            "downloaded_from": "google drive",
            "error": "unknown content type: {'text/bla bla'}",
            "source_url": url,
        }


def test_extract_gdrive_contents_unknown_content_type():
    res = Mock(headers={"Content-Type": "bla bla"}, status_code=200)
    url = "https://drive.google.com/file/d/1OrKZlksba2a8gKa5bAQfP2qF717O_57I/view?usp=sharing"
    
    with patch("align_data.sources.articles.google_cloud.fetch", return_value=res):
        assert extract_gdrive_contents(url) == {
            "downloaded_from": "google drive",
            "source_url": url,
            "error": "unknown content type: {'bla bla'}",
        }
