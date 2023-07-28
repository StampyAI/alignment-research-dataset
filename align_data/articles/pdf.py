import io
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin

from dateutil.parser import parse
import requests
import pandas as pd
from PyPDF2 import PdfReader # TODO: replace with pypdf
from PyPDF2.errors import PdfReadError # TODO: replace with pypdf

from align_data.articles.html import fetch, fetch_element
from logger_config import logger


def sci_hub_pdf(identifier):
    """Search Sci-hub for a link to a pdf of the article with the given identifier.

    This will only get pdf that are directly served by Sci-hub. Sometimes it will redirect to a
    large file containing multiple articles, e.g. a whole journal or book, in which case this function
    will ignore the result.
    """
    elem = fetch_element(f'https://sci-hub.st/{identifier}', 'embed')
    if not elem:
        return None
    src = elem.get('src').strip()
    if src.startswith('//'):
        src = 'https:' + src
    elif src.startswith('/'):
        src = f'https://sci-hub.st/{src}'
    return src


def read_pdf(filename):
    try:
        pdf_reader = PdfReader(filename)
        return '\n'.join(page.extract_text() for page in pdf_reader.pages)
    except PdfReadError as e:
        logger.error(e)
    return None


def fetch_pdf(link):
    """Return the contents of the pdf file at `link` as a markdown string.

    :param str link: the URL to check for a pdf file
    :returns: the contents of the pdf file as markdown."""
    res = fetch(link)
    if res.status_code >= 400:
        logger.error('Could not fetch the pdf file at %s - are you sure that link is correct?', link)

    content_type = {c_type.strip().lower() for c_type in res.headers.get('Content-Type').split(';')}
    if not content_type & {'application/octet-stream', 'application/pdf'}:
        return {
            'error': f'Wrong content type retrieved: {content_type} - {link}',
            'contents': res.content,
        }

    try:
        pdf_reader = PdfReader(io.BytesIO(res.content))
        return {
            'source_url': link,
            'text': '\n'.join(page.extract_text() for page in pdf_reader.pages),
            'data_source': 'pdf',
        }
    except PdfReadError as e:
        logger.error('Could not read PDF file: %s', e)
        return {'error': str(e)}


def get_arxiv_link(doi):
    """Find the URL to the pdf of the given arXiv DOI."""
    res = requests.get(f"https://doi.org/api/handles/{doi}")
    if res.status_code != 200:
        return None

    vals = [i for i in response.json().get('values') if i.get('type', '').upper() == 'URL']
    if not vals:
        return None
    return vals[0]["data"]["value"].replace("/abs/", "/pdf/") + ".pdf"


def get_arxiv_pdf(link):
    return fetch_pdf(link.replace('/abs/', '/pdf/'))


def get_doi(doi):
    """Get the article with the given `doi`.

    This will look for it in sci-hub and arxiv (if applicable), as those are likely the most
    comprehensive sources of pdfs.
    """
    if 'arXiv' in doi:
        link = get_arxiv_link(doi)
        pdf = (link and fetch_pdf(link))
        if pdf and 'text' in pdf:
            pdf['downloaded_from'] = 'arxiv'
            return pdf

    if link := sci_hub_pdf(doi):
        if pdf := fetch_pdf(link):
            pdf['downloaded_from'] = 'scihub'
            return pdf
    return {'error': 'Could not find pdf of article by DOI'}


def doi_getter(url):
    """Extract the DOI from the given `url` and fetch the contents of its article."""
    return get_doi(urlparse(url).path.lstrip('/'))


def get_pdf_from_page(*link_selectors):
    """Get a function that receives an `url` to a page containing a pdf link and returns the pdf's contents as text.

    Starting from `url`, fetch the contents at the URL, extract the link using a CSS selector, then:
     * if there are more selectors left, fetch the contents at the extracted link and continue
     * otherwise return the pdf contents at the last URL

    :param List[str] link_selectors: CSS selector used to find the final download link
    :returns: the contents of the pdf file as a string
    """
    def getter(url):
        link = url
        for selector in link_selectors:
            elem = fetch_element(link, selector)
            if not elem:
                return {'error': f'Could not find pdf download link for {link} using \'{selector}\''}

            link = elem.get('href')
            if not link.startswith('http') or not link.startswith('//'):
                link = urljoin(url, link)

        # Some pages keep link to google drive previews of pdf files, which need to be
        # mangled to get the URL of the actual pdf file
        # TODO: circular dependency
        if 'drive.google.com' in link and '/view' in link:
            return extract_gdrive_contents(link)

        if pdf := fetch_pdf(link):
            return pdf
        return {'error': f'Could not fetch pdf from {link}'}
    return getter
