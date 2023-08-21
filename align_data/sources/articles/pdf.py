import io
import logging
from typing import Dict, Any, List
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, Any

import requests
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError
from markdownify import MarkdownConverter
from bs4.element import Tag

from align_data.sources.articles.html import fetch, fetch_element, with_retry

logger = logging.getLogger(__name__)


def sci_hub_pdf(identifier: str) -> str | None:
    """Search Sci-hub for a link to a pdf of the article with the given identifier (doi).

    This will only get pdf that are directly served by Sci-hub. Sometimes it will redirect to a
    large file containing multiple articles, e.g. a whole journal or book, in which case this function
    will ignore the result.
    """
    elem = fetch_element(f"https://sci-hub.st/{identifier}", "embed")
    if not elem:
        return None
    
    src = elem.get("src")

    if isinstance(src, list):
        src = src[0] if src else None
    
    if src is None:
        return None
    
    src = src.strip()
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = f"https://sci-hub.st/{src}"
    return src


def read_pdf(filename: Path) -> str | None:
    try:
        pdf_reader = PdfReader(filename)
        return "\n".join(page.extract_text() for page in pdf_reader.pages)
    except PdfReadError as e:
        logger.error(e)
    return None


@with_retry(times=3)
def fetch_pdf(link: str) -> Dict[str, Any]:
    """Return the contents of the pdf file at `link` as a markdown string.

    :param str link: the URL to check for a pdf file
    :returns: the contents of the pdf file as markdown."""
    res = fetch(link)
    if res.status_code >= 400:
        logger.error(
            "Could not fetch the pdf file at %s - are you sure that link is correct?",
            link,
        )
        return {"error": "Could not read pdf file"}

    content_type = {
        c_type.strip().lower() for c_type in res.headers.get("Content-Type", "").split(";")
    }
    if not content_type & {"application/octet-stream", "application/pdf"}:
        return {
            "error": f"Wrong content type retrieved: {content_type} - {link}",
            "contents": res.content.decode('utf-8'),
        }

    try:
        pdf_reader = PdfReader(io.BytesIO(res.content))
        return {
            "source_url": link,
            "text": "\n".join(page.extract_text() for page in pdf_reader.pages),
            "source_type": "pdf",
        }
    except (TypeError, PdfReadError) as e:
        logger.error('Could not read PDF file: %s', e)
        error = str(e)

    filenames = [
        i.strip().split("=")[1]
        for i in res.headers.get("Content-Disposition", "").split(";")
        if "filename" in i
    ]
    if filenames and "pdf" not in filenames[0].lower():
        logger.error(
            "Are you sure %s points to a pdf file? The response says the file should be called %s",
            link,
            filenames[0],
        )
        error = f"Probably bad file type: {filenames[0]} - {link}"

    return {"error": error}


def get_arxiv_link(doi: str) -> str | None:
    """Find the URL to the pdf of the given arXiv DOI."""
    res = requests.get(f"https://doi.org/api/handles/{doi}")
    if res.status_code != 200:
        return None

    vals = [
        val
        for val in res.json().get("values")
        if val.get("type", "").upper() == "URL"
    ]

    if not vals:
        return None
    return vals[0]["data"]["value"].replace("/abs/", "/pdf/") + ".pdf"


def get_doi(doi: str) -> Dict[str, Any]:
    """Get the article with the given `doi`.

    This will look for it in sci-hub and arxiv (if applicable), as those are likely the most
    comprehensive sources of pdfs.
    """
    if "arXiv" in doi:
        link = get_arxiv_link(doi)
        pdf = link and fetch_pdf(link)
        if pdf and "text" in pdf:
            return {**pdf, "downloaded_from": "arxiv"}

    if link := sci_hub_pdf(doi):
        if pdf := fetch_pdf(link):
            return {**pdf, "downloaded_from": "scihub"}
    return {"error": "Could not find pdf of article by DOI"}


def doi_getter(url: str) -> Dict[str, Any]:
    """Extract the DOI from the given `url` and fetch the contents of its article."""
    return get_doi(urlparse(url).path.lstrip("/"))


def parse_vanity(url: str) -> Dict[str, Any]:
    contents = fetch_element(url, "article")
    if not contents:
        return {'error': 'Could not fetch from arxiv vanity'}

    selected_title = contents.select_one("h1.ltx_title")
    title = selected_title.text if selected_title else None

    def get_first_child(item: Tag) -> List[str]:
        child = next(iter(item.children), None)
        if not child:
            return []
        return child.text.split(",")

    authors = [
        author.strip()
        for item in contents.select("div.ltx_authors .ltx_personname")
        for author in get_first_child(item)
    ]

    selected_date = contents.select_one("div.ltx_dates")
    date_published = selected_date.text.strip("()") if selected_date else None

    text = "\n\n".join(
        MarkdownConverter().convert_soup(elem).strip()
        for elem in contents.select("section.ltx_section")
    )

    return {
        "title": title,
        "authors": authors,
        "text": text,
        "date_published": date_published,
        "source_type": "html",
    }
