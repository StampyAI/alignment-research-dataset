import io
import logging
from urllib.parse import urlparse

import requests
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError
from markdownify import MarkdownConverter

from align_data.sources.articles.html import fetch, fetch_element, with_retry

logger = logging.getLogger(__name__)


def sci_hub_pdf(identifier):
    """Search Sci-hub for a link to a pdf of the article with the given identifier.

    This will only get pdf that are directly served by Sci-hub. Sometimes it will redirect to a
    large file containing multiple articles, e.g. a whole journal or book, in which case this function
    will ignore the result.
    """
    elem = fetch_element(f"https://sci-hub.st/{identifier}", "embed")
    if not elem:
        return None
    src = elem.get("src").strip()
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = f"https://sci-hub.st/{src}"
    return src


def read_pdf(filename):
    try:
        pdf_reader = PdfReader(filename)
        return "\n".join(page.extract_text() for page in pdf_reader.pages)
    except PdfReadError as e:
        logger.error(e)
    return None


@with_retry(times=3)
def fetch_pdf(link):
    """Return the contents of the pdf file at `link` as a markdown string.

    :param str link: the URL to check for a pdf file
    :returns: the contents of the pdf file as markdown."""
    res = fetch(link)
    if res.status_code >= 400:
        logger.error(
            "Could not fetch the pdf file at %s - are you sure that link is correct?",
            link,
        )

    content_type = {
        c_type.strip().lower() for c_type in res.headers.get("Content-Type").split(";")
    }
    if not content_type & {"application/octet-stream", "application/pdf"}:
        return {
            "error": f"Wrong content type retrieved: {content_type} - {link}",
            "contents": res.content,
        }

    try:
        pdf_reader = PdfReader(io.BytesIO(res.content))
        return {
            "source_url": link,
            "text": "\n".join(page.extract_text() for page in pdf_reader.pages),
            "data_source": "pdf",
        }
    except (TypeError, PdfReadError) as e:
        logger.error('Could not read PDF file: %s', e)
        return {'error': str(e)}

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


def get_arxiv_link(doi):
    """Find the URL to the pdf of the given arXiv DOI."""
    res = requests.get(f"https://doi.org/api/handles/{doi}")
    if res.status_code != 200:
        return None

    vals = [
        val
        for val in response.json().get("values")
        if val.get("type", "").upper() == "URL"
    ]

    if not vals:
        return None
    return vals[0]["data"]["value"].replace("/abs/", "/pdf/") + ".pdf"


def get_arxiv_pdf(link):
    return fetch_pdf(link.replace("/abs/", "/pdf/"))


def get_doi(doi):
    """Get the article with the given `doi`.

    This will look for it in sci-hub and arxiv (if applicable), as those are likely the most
    comprehensive sources of pdfs.
    """
    if "arXiv" in doi:
        link = get_arxiv_link(doi)
        pdf = link and fetch_pdf(link)
        if pdf and "text" in pdf:
            pdf["downloaded_from"] = "arxiv"
            return pdf

    if link := sci_hub_pdf(doi):
        if pdf := fetch_pdf(link):
            pdf["downloaded_from"] = "scihub"
            return pdf
    return {"error": "Could not find pdf of article by DOI"}


def doi_getter(url):
    """Extract the DOI from the given `url` and fetch the contents of its article."""
    return get_doi(urlparse(url).path.lstrip("/"))


def parse_vanity(url):
    contents = fetch_element(url, "article")
    if not contents:
        return None

    if title := contents.select_one("h1.ltx_title"):
        title = title.text

    def get_first_child(item):
        child = next(item.children)
        if not child:
            return []

        if not isinstance(child, str):
            child = child.text
        return child.split(",")

    authors = [
        a.strip()
        for item in contents.select("div.ltx_authors .ltx_personname")
        for a in get_first_child(item)
    ]

    if date_published := contents.select_one("div.ltx_dates"):
        date_published = date_published.text.strip("()")

    text = "\n\n".join(
        MarkdownConverter().convert_soup(elem).strip()
        for elem in contents.select("section.ltx_section")
    )

    return {
        "title": title,
        "authors": authors,
        "text": text,
        "date_published": date_published,
        "data_source": "html",
    }
