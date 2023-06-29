import io
import logging
from typing import List, Dict
from urllib.parse import urlparse, urljoin

import regex as re
import requests
from bs4 import BeautifulSoup
from markdownify import MarkdownConverter, markdownify
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

logger = logging.getLogger(__name__)


def fetch(url, method='get'):
    """Fetch the given `url`.

    This function is to have a single place to manage headers etc.
    """
    return getattr(requests, method)(
        url, allow_redirects=True,
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0',
        }
    )


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


def fetch_pdf(link):
    """Return the contents of the pdf file at `link` as a markdown string.

    :param str link: the URL to check for a pdf file
    :returns: the contents of the pdf file as markdown."""
    res = fetch(link)
    if res.status_code >= 400:
        logger.error('Could not fetch the pdf file at %s - are you sure that link is correct?', link)

    content_type = {c_type.strip().lower() for c_type in res.headers.get('Content-Type').split(';')}
    if not content_type & {'application/octet-stream', 'application/pdf'}:
        return {'error': f'Wrong content type retrieved: {content_type} - {link}'}

    try:
        pdf_reader = PdfReader(io.BytesIO(res.content))
        return {'text': '\n'.join(page.extract_text() for page in pdf_reader.pages)}
    except PdfReadError as e:
        logger.error('Could not read PDF file: %s', e)
        return {'error': str(e)}

    filenames = [
        i.strip().split('=')[1]
        for i in res.headers.get('Content-Disposition', '').split(';')
        if 'filename' in i
    ]
    if filenames and 'pdf' not in filenames[0].lower():
        logger.error('Are you sure %s points to a pdf file? The response says the file should be called %s', link, filenames[0])
        error = f'Probably bad file type: {filenames[0]} - {link}'

    return {'error': error}


def fetch_element(url, selector):
    """Fetch the first HTML element that matches the given CSS `selector` on the page found at `url`."""
    try:
        resp = fetch(url)
    except requests.exceptions.ConnectionError:
        logger.error('Could not connect to %s', url)
        return None

    soup = BeautifulSoup(resp.content, "html.parser")
    return soup.select_one(selector)


def element_extractor(selector, remove=[]):
    """Returns a function that will extract the first element that matches the given CSS selector.

    :params str selector: a CSS selector to run on the HTML of the page provided as the parameter of the function
    :param List[str] remove: An optional list of selectors to be removed from the resulting HTML. Useful for removing footers etc.
    :returns: A function that expects to get an URL, and which will then return the contents of the selected HTML element as markdown.
    """
    def getter(url):
        elem = fetch_element(url, selector)
        if not elem:
            return {'error': 'could not find HTML element'}

        for sel in remove:
            for e in elem.select(sel):
                e.extract()
        return {
            'text': MarkdownConverter().convert_soup(elem).strip(),
            'data_source': 'html',
            'source_url': url,
        }

    return getter


def medium_blog(url):
    """Return the contents of the medium article at the given URL as markdown."""
    article = fetch_element(url, 'article')
    article.find('h1').parent.extract()  # remove the header
    if article:
        return {
            'text': MarkdownConverter().convert_soup(article).strip(),
            'data_source': 'html',
            'source_url': url,
        }
    return {'error': 'Could not final article in HTML'}


def get_arxiv_link(doi):
    """Find the URL to the pdf of the given arXiv DOI."""
    res = requests.get(f"https://doi.org/api/handles/{doi}")
    if res.status_code != 200:
        return None

    vals = [i for i in response.json().get('values') if i.get('type', '').upper() == 'URL']
    if not vals:
        return None
    return vals[0]["data"]["value"].replace("/abs/", "/pdf/") + ".pdf"


def get_doi(doi):
    """Get the article with the given `doi`.

    This will look for it in sci-hub and arxiv (if applicable), as those are likely the most
    comprehensive sources of pdfs.
    """
    if 'arXiv' in doi:
        link = get_arxiv_link(doi)
        pdf = (link and fetch_pdf(link))
        if pdf and 'text' in pdf:
            return {
                'text': pdf['text'],
                'source_url': link,
                'data_source': 'pdf',
                'downloaded_from': 'arXiv',
            }

    if link := sci_hub_pdf(doi):
        if pdf := fetch_pdf(link):
            if 'text' in pdf:
                return {
                    'text': pdf,
                    'source_url': link,
                    'data_source': 'pdf',
                    'downloaded_from': 'scihub',
                }
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
        if 'drive.google.com' in link and '/view' in link:
            return extract_gdrive_contents(link)

        pdf = fetch_pdf(link)
        if not pdf:
            return {'error': f'Could not fetch pdf from {link}'}
        if 'error' in pdf:
            return pdf
        return {
            'source_url': link,
            'data_source': 'pdf',
            'text': pdf.get('text'),
        }
    return getter


def google_doc(url: str) -> str:
    """Fetch the contents of the given gdoc url as markdown."""
    res = re.search(r'https://docs.google.com/document/(?:u/)?(?:0/)?d/(.*?)/', url)
    if not res:
        return {'error': f'Could not find google doc id from url: {url}'}

    doc_id = res.group(1)
    body = fetch_element(f'https://docs.google.com/document/d/{doc_id}/export?format=html', 'body')
    if body:
        return {
            'source_url': url,
            'data_source': 'google docs',
            'text': MarkdownConverter().convert_soup(body).strip(),
        }
    return {'error': 'Could not extract text from google doc'}


def extract_gdrive_contents(link):
    file_id = link.split('/')[-2]
    url = f'https://drive.google.com/uc?id={file_id}'
    res = fetch(url, 'head')
    if res.status_code >= 400:
        logger.error('Could not fetch the pdf file at %s - are you sure that link is correct?', link)
        return {'error': 'Could not read file from google drive'}

    result = {
        'source_url': link,
        'downloaded_from': 'google drive',
    }

    content_type = {c_type.strip().lower() for c_type in res.headers.get('Content-Type').split(';')}
    if not content_type:
        result['error'] = 'no content type'
    elif content_type & {'application/octet-stream', 'application/pdf'}:
        result.update(fetch_pdf(url))
    elif content_type & {'application/epub+zip', 'application/epub'}:
        result['data_source'] = 'ebook'
    else:
        result['error'] = f'unknown content type: {content_type}'

    return result


def none_with_error(error):
    """Returns a url handler function that just logs the provided `error` string."""
    return lambda url: error and logger.error(error)


def multistrategy(*funcs):
    """Merges multiple getter functions, returning the result of the first function call to succeed."""
    def getter(url):
        for func in funcs:
            res = func(url)
            if res and 'error' not in res:
                return res
    return getter


PARSERS = {
    # Unhandled items that will be caught later. Though it would be good for them also to be done properly
    'oxford.universitypressscholarship.com': none_with_error(''),

    # Paywalled journal
    'linkinghub.elsevier.com': none_with_error('Elsevier is a known parasite - no point in looking to them for content'),
    'www.dl.begellhouse.com': none_with_error('This article is paywalled'),

    # Domain sepecific handlers
    'apcz.umk.pl': get_pdf_from_page('.galleys_links a.pdf', 'a.download'),
    'academic.oup.com': multistrategy(get_pdf_from_page('a.article-pdfLink'), element_extractor('#ContentTab')),
    'ai.googleblog.com': element_extractor('div.post-body.entry-content'),
    'bair.berkeley.edu': element_extractor('article'),
    'mediangroup.org': element_extractor('div.entry-content'),
    'www.alexirpan.com': element_extractor('article'),
    'www.incompleteideas.net': element_extractor('body'),
    'ai-alignment.com': medium_blog,
    'aisrp.org': element_extractor('article'),
    'bounded-regret.ghost.io': element_extractor('div.post-content'),
    'carnegieendowment.org': element_extractor('div.article-body', remove=['.no-print', '.related-pubs']),
    'casparoesterheld.com': element_extractor('.entry-content', remove=['div.sharedaddy']),
    'cset.georgetown.edu': get_pdf_from_page('a:-soup-contains("Download Full")'),
    'cullenokeefe.com': element_extractor('div.sqs-block-content'),
    'deepmindsafetyresearch.medium.com': medium_blog,
    'docs.google.com': google_doc,
    'docs.microsoft.com': element_extractor('div.content'),
    'drive.google.com': extract_gdrive_contents,
    'doi.org': doi_getter,
    'dl.acm.org': fetch_pdf,
    'dspace.mit.edu': get_pdf_from_page('a.btn-primary.download-button'),
    'digichina.stanford.edu': element_extractor('div.h_editor-content'),
    'en.wikipedia.org': element_extractor('main.mw-body'),
    'eng.uber.com': element_extractor('div.article-body'),
    'futureoflife.org': multistrategy(
        element_extractor('div.body-content'),
        element_extractor('#main-content'),
    ),
    'gcrinstitute.org': element_extractor('div.blog-content'),
    'globalprioritiesinstitute.org': get_pdf_from_page('a:-soup-contains("PDF")'),
    'jbkjr.me': element_extractor('section.page__content'),
    'link.springer.com': multistrategy(
        element_extractor('article.c-article-body'),
        get_pdf_from_page('div.c-pdf-download a'),
        doi_getter,
        none_with_error('This article looks paywalled'),
    ),
    'longtermrisk.org': element_extractor('div.entry-content'),
    'lukemuehlhauser.com': element_extractor('div.entry-content'),
    'medium.com': medium_blog,
    'openaccess.thecvf.com': get_pdf_from_page('a:-soup-contains("pdf")'),
    'openai.com': element_extractor('#content'),
    'openreview.net': get_pdf_from_page('a.note_content_pdf'),
    'ora.ox.ac.uk': fetch_pdf,
    'ought.org': element_extractor('div.BlogPostBodyContainer'),
    'papers.nips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'papers.ssrn.com': get_pdf_from_page('.abstract-buttons a.button-link:-soup-contains("Download")'),
    'par.nsf.gov': get_pdf_from_page('a:-soup-contains("Accepted Manuscript")'),
    'proceedings.neurips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'psyarxiv.com': lambda url: fetch_pdf(url.rstrip('/') + '/download'),
    'rowanzellers.com': get_pdf_from_page('main a:-soup-contains("Paper")'),
    'sideways-view.com': element_extractor('article', remove=['header']),
    'slatestarcodex.com': element_extractor('div.pjgm-postcontent'),
    'techpolicy.press': element_extractor('div.post-content', remove=['div.before_content', '.sabox-guest-authors-container', '.jp-relatedposts']),
    'theconversation.com': element_extractor('div.content-body'),
    'thegradient.pub': element_extractor('div.c-content'),
    'towardsdatascience.com': medium_blog,
    'unstableontology.com': element_extractor('.entry-content', remove=['div.sharedaddy']),
    'waitbutwhy.com': element_extractor('article', remove=['.entry-header']),
    'weightagnostic.github.io': element_extractor('dt-article', remove=['#authors_section', 'dt-byline']),
    'cnas.org': element_extractor('#mainbar-toc'),
    'econlib.org': element_extractor('div.post-content'),
    'humanityplus.org': element_extractor('div.content'),
    'gleech.org': element_extractor('article.post-content', remove=['center', 'div.accordion']),
    'governance.ai': get_pdf_from_page('a.read-paper-button:not([href="#"])'),
    'ibm.com': element_extractor('div:has(> p)'),  # IBM's HTML is really ugly...
    'ijcai.org': get_pdf_from_page('a.btn-download:-soup-contains("PDF")'),
    'jair.org': get_pdf_from_page('div.download a.pdf', 'a.download'),
    'jstor.org': doi_getter,
    'microsoft.com': element_extractor('div.content-container'),
    'mdpi.com': element_extractor(
        'article', remove=[
            '.article-icons', '.title', '.art-authors', '.art-affiliations', '.bib-identity',
            '.pubhistory', '.belongsTo', '.highlight-box1', '.additional-content'
        ]
    ),
    'nature.com': element_extractor('article', remove=['header', '#rightslink-section', '#article-info-section']),
    'ncbi.nlm.nih.gov': element_extractor('div.article'),
    'openphilanthropy.org': element_extractor('div.pagenav-content'),
    'ri.cmu.edu': get_pdf_from_page('a.pub-link'),
    'risksciences.ucla.edu': get_pdf_from_page('a:-soup-contains("Download")'),
    'safe.ai': element_extractor('#open-letter'),
    'sciencedirect.com': element_extractor(
        'article',
        remove=[
            '#section-cited-by', '.Copyright', '.issue-navigation', '.ReferencedArticles',
            '.LicenseInfo', '.ArticleIdentifierLinks', '.Banner', '.screen-reader-main-title', '.Publication'
        ]
    ),
    'ssrn.com': get_pdf_from_page('.abstract-buttons a.button-link:-soup-contains("Download")'),
    'vox.com': element_extractor('did.c-entry-content', remove=['c-article-footer']),
    'weforum.org': element_extractor('div.wef-0'),
    'www6.inrae.fr': element_extractor('div.ArticleContent'),
    'aleph.se': element_extractor('body'),
    'yjolt.org': get_pdf_from_page('span.file a'),
    'yoshuabengio.org': element_extractor('div.post-content'),

    # To be implemented
    'goodreads.com': none_with_error('Ebooks are not yet handled'),
    'judiciary.senate.gov': none_with_error(''),
    'taylorfrancis.com': none_with_error('Ebooks are not yet handled'),
    'youtube.com': none_with_error('Youtube videos are not yet handled'),
    'researchgate.net': none_with_error('Researchgate makes it hard to auto download pdf - please provide a DOI or a different url to the contents'),
    'repository.cam.ac.uk': none_with_error(''),
}


def extract_text(url):
    """Get the contents at the given `url`."""
    # Check if the domain has a specific handler defined
    domain = urlparse(url).netloc.lstrip('www.')
    if parser := PARSERS.get(domain):
        parsed = parser(url)
        if parsed and 'error' not in parsed:
            # Successfully parsed - this can be returned and the children rejoice
            return parsed

    # Check if the url is to a pdf - it might not be, as this is just a wild guess
    pdf = fetch_pdf(url)

    # It was a pdf - good, this can also be returned
    if pdf and 'text' in pdf:
        return {
            'source_url': url,
            'data_source': 'pdf',
            'text': pdf.get('text'),
            'downloaded_from': 'default parser',
        }

    # It looked like a pdf (or something akin), but couldn't be parsed properly - return its error
    if pdf and not pdf.get('error').startswith('Wrong content type retrieved'):
        return pdf

    # At this point we've established both that the url couldn't be properly parsed by a parser, and it's
    # also not a valid pdf.
    if not parser:
        logger.error('No handler defined for %s - please add one, or change the url (%s)', domain, url)
        return {'error': 'No domain handler defined'}

    # Assume that a generic error happened
    return None
