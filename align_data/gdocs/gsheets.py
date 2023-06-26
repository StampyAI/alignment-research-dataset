import io
import logging
from dataclasses import dataclass, field
from typing import List, Dict
from urllib.parse import urlparse, urljoin

import pandas as pd
import regex as re
import requests
from bs4 import BeautifulSoup
from markdownify import MarkdownConverter, markdownify
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

logger = logging.getLogger(__name__)


def fetch(url):
    """Fetch the given `url`.

    This function is to have a single place to manage headers etc.
    """
    return requests.get(
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


def extract_pdf(link):
    """Return the contents of the pdf file at `link` as a markdown string.

    :param str link: the URL to check for a pdf file
    :returns: the contents of the pdf file as markdown."""
    res = fetch(link)
    if res.status_code >= 400:
        logger.error('Could not fetch the pdf file at %s - are you sure that link is correct?', link)

    # Handle cases like 'application/pdf; header=present'
    content_type = {c_type.strip().lower() for c_type in res.headers.get('Content-Type').split(';')}
    if not content_type & {'application/octet-stream', 'application/pdf'}:
        return None

    try:
        pdf_reader = PdfReader(io.BytesIO(res.content))
        return '\n'.join(page.extract_text() for page in pdf_reader.pages)
    except PdfReadError as e:
        logger.error('Could not read PDF file: %s', e)

    filenames = [
        i.strip().split('=')[1]
        for i in res.headers.get('Content-Disposition', '').split(';')
        if 'filename' in i
    ]
    if filenames and 'pdf' not in filenames[0].lower():
        logger.error('Are you sure %s points to a pdf file? The response says the file should be called %s', link, filenames[0])

    return None


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
            return None

        for sel in remove:
            for e in elem.select(sel):
                e.extract()
        return MarkdownConverter().convert_soup(elem).strip()
    return getter


def medium_blog(url):
    """Return the contents of the medium article at the given URL as markdown."""
    article = fetch_element(url, 'article')
    article.find('h1').parent.extract()  # remove the header
    return article and MarkdownConverter().convert_soup(article).strip()


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
        if pdf := (link and extract_pdf(link)):
            return pdf

    if link := sci_hub_pdf(doi):
        return extract_pdf(link)
    return None


def doi_getter(url):
    """Extract the DOI from the given `url` and fetch the contents of its article."""
    return get_doi(urlparse(url).path.lstrip('/'))


def get_google_drive_pdf(link):
    file_id = link.split('/')[-2]
    return extract_pdf(f'https://drive.google.com/uc?id={file_id}')


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
                return None

            link = elem.get('href')
            if not link.startswith('http') or not link.startswith('//'):
                link = urljoin(url, link)

        # Some pages keep link to google drive previews of pdf files, which need to be
        # mangled to get the URL of the actual pdf file
        if 'drive.google.com' in link and '/view' in link:
            return get_google_drive_pdf(link)

        return extract_pdf(link)
    return getter


def google_doc(url: str) -> str:
    """Fetch the contents of the given gdoc url as markdown."""
    res = re.search(r'https://docs.google.com/document/(?:u/)?(?:0/)?d/(.*?)/', url)
    if not res:
        return None

    doc_id = res.group(1)
    body = fetch_element(f'https://docs.google.com/document/d/{doc_id}/export?format=html', 'body')
    return body and MarkdownConverter().convert_soup(body).strip()


def none_with_error(error):
    """Returns a url handler function that just logs the provided `error` string."""
    return lambda url: error and logger.error(error)


def multistrategy(*funcs):
    """Merges multiple getter functions, returning the result of the first function call to succeed."""
    def getter(url):
        for func in funcs:
            if res := func(url):
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
    'drive.google.com': get_google_drive_pdf,
    'doi.org': doi_getter,
    'dl.acm.org': extract_pdf,
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
    'ora.ox.ac.uk': extract_pdf,
    'ought.org': element_extractor('div.BlogPostBodyContainer'),
    'papers.nips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'papers.ssrn.com': get_pdf_from_page('.abstract-buttons a.button-link:-soup-contains("Download")'),
    'par.nsf.gov': get_pdf_from_page('a:-soup-contains("Accepted Manuscript")'),
    'proceedings.neurips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'psyarxiv.com': lambda url: extract_pdf(url.rstrip('/') + '/download'),
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
    'www.cnas.org': element_extractor('#mainbar-toc'),
    'www.econlib.org': element_extractor('div.post-content'),
    'www.humanityplus.org': element_extractor('div.content'),
    'www.gleech.org': element_extractor('article.post-content', remove=['center', 'div.accordion']),
    'www.governance.ai': get_pdf_from_page('a.read-paper-button:not([href="#"])'),
    'www.ibm.com': element_extractor('div:has(> p)'),  # IBM's HTML is really ugly...
    'www.ijcai.org': get_pdf_from_page('a.btn-download:-soup-contains("PDF")'),
    'www.jair.org': get_pdf_from_page('div.download a.pdf', 'a.download'),
    'www.jstor.org': doi_getter,
    'www.microsoft.com': element_extractor('div.content-container'),
    'www.mdpi.com': element_extractor(
        'article', remove=[
            '.article-icons', '.title', '.art-authors', '.art-affiliations', '.bib-identity',
            '.pubhistory', '.belongsTo', '.highlight-box1', '.additional-content'
        ]
    ),
    'www.nature.com': element_extractor('article', remove=['header', '#rightslink-section', '#article-info-section']),
    'www.ncbi.nlm.nih.gov': element_extractor('div.article'),
    'www.openphilanthropy.org': element_extractor('div.pagenav-content'),
    'www.ri.cmu.edu': get_pdf_from_page('a.pub-link'),
    'www.risksciences.ucla.edu': get_pdf_from_page('a:-soup-contains("Download")'),
    'www.safe.ai': element_extractor('#open-letter'),
    'www.sciencedirect.com': element_extractor(
        'article',
        remove=[
            '#section-cited-by', '.Copyright', '.issue-navigation', '.ReferencedArticles',
            '.LicenseInfo', '.ArticleIdentifierLinks', '.Banner', '.screen-reader-main-title', '.Publication'
        ]
    ),
    'www.ssrn.com': get_pdf_from_page('.abstract-buttons a.button-link:-soup-contains("Download")'),
    'www.vox.com': element_extractor('did.c-entry-content', remove=['c-article-footer']),
    'www.weforum.org': element_extractor('div.wef-0'),
    'www6.inrae.fr': element_extractor('div.ArticleContent'),
    'www.aleph.se': element_extractor('body'),
    'yjolt.org': get_pdf_from_page('span.file a'),
    'yoshuabengio.org': element_extractor('div.post-content'),

    # To be implemented
    'www.goodreads.com': none_with_error('Ebooks are not yet handled'),
    'www.judiciary.senate.gov': none_with_error(''),
    'www.taylorfrancis.com': none_with_error('Ebooks are not yet handled'),
    'www.youtube.com': none_with_error('Youtube videos are not yet handled'),
    'www.researchgate.net': none_with_error('Researchgate makes it hard to auto download pdf - please provide a DOI or a different url to the contents'),
    'www.repository.cam.ac.uk': none_with_error(''),
}


def extract_text(url):
    """Get the contents at the given `url`."""
    # First check if the domain has a specific handler defined
    domain = urlparse(url).netloc
    if parser := PARSERS.get(domain):
        return parser(url)

    # Check if the url is to a pdf
    if pdf := extract_pdf(url):
        return pdf

    logger.error('No handler defined for %s - please add one, or change the url (%s)', domain, url)


# TODO: These are domains that need handlers added
PROBLEMATICAL_DOMAINS = [
    'www.ssrn.com',  # pdf downloads work on the page, but not via requests
    'papers.ssrn.com',
    'www.jstor.org',  # JSTOR pages require accepting their terms before downloading
    'www.judiciary.senate.gov', # not yet implemented
    'www.goodreads.com',  # not yet implemented
    'www.repository.cam.ac.uk', # requests gets a 502 error - needs investigating
    'www.researchgate.net',  # researchgate needs JS to render the page properly
    'www.youtube.com',  # not yet implemented
]


@dataclass
class GSheets(AlignmentDataset):

    spreadsheet_id: str
    sheet_id: str
    mappings: Dict[str, str] = field(default_factory=dict)
    extra_fields: List[str] = field(default_factory=list)
    done_key = "url"

    @property
    def items_list(self):
        logger.info(f'Fetching https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.sheet_id}')
        df = pd.read_csv(f'https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.sheet_id}')
        return (item for item in df.itertuples() if not pd.isna(self.get_item_key(item)))

    def get_value(self, item, key):
        """Get the value of the given `key` in `item`.

        There are certain keys that are assumed to be present in every item, but they can have a different
        name in the actual csv file. The `self.mappings` properly has a dict of mappings - any key that is
        not in that dict is assumed to be the same.
        """
        if mapped_key := self.mappings.get(key):
            return getattr(item, mapped_key, None)
        return getattr(item, key, None)

    def get_item_key(self, item):
        return self.get_value(item, self.done_key)

    def get_text(self, item):
        """Extract the text for the given `item`.

        Each item has an `url` set, which should point to the contents, but if that doesn't return
        anything, then try alternative ways, e.g. searching for it by DOI, if provided
        """
        # if the url ends with '.pdf', then that's most likely a direct link to a pdf, so try downloading
        # it before faffing around with domain specific handlers
        url = self.get_item_key(item)
        if urlparse(url).path.lower().endswith('.pdf'):
            return extract_pdf(url)

        # If the item has a doi set, check if its contents can be found by searching for it
        doi = self.get_value(item, 'DOI')
        if doi and not pd.isna(doi):
            text = get_doi(doi)
            if text:
                return text

        # Otherwise just try the domain specific handler
        return extract_text(url)

    def process_entry(self, item):
        url = self.get_item_key(item)
        text = self.get_text(item)

        if not text:
            self.log_problem_with_fetching_text(item)
            return None

        summary = [self.get_value(item, key) for key in self.mappings.get('summary', [])]
        summary = [val.strip() for val in summary if val and not pd.isna(val) and val.strip()]

        authors = []
        raw_authors = self.get_value(item, 'authors')
        if raw_authors and not pd.isna(raw_authors):
            authors = [author.strip() for author in raw_authors.split(',')]

        return DataEntry({
            "source": self.name,
            "source_type": self.get_value(item, 'source_type'),
            "title": self.get_value(item, 'title'),
            "authors": authors,
            "date_published": self.get_value(item, 'date_published'),
            "text": text,
            "url": url,
            "summary": summary,
        }, **{extra_field: self.get_value(item, extra_field) for extra_field in self.extra_fields})

    def log_problem_with_fetching_text(self, item):
        title = self.get_value(item, 'title')
        url = self.get_item_key(item)
        domain = urlparse(url).netloc

        if self.get_value(item, 'source_type') in ['bookSection', 'book']:
            logger.error('Could not process "%s", as it\'s is a book section - is there a direct link to a pdf of it that could be provided?', title)
        elif domain in PROBLEMATICAL_DOMAINS:
            logger.error('Could not process "%s": %s pages are not handled properly yet - is there a direct link to a pdf that can be provided instead of "%s"?', title, domain, url)
        else:
            logger.error('Could not process "%s": %s', title, url)
