import io
import logging
from dataclasses import dataclass, field
from typing import List, Dict
from urllib.parse import urlparse

import pandas as pd
import regex as re
import requests
from bs4 import BeautifulSoup
from markdownify import MarkdownConverter, markdownify
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

from align_data.common.alignment_dataset import AlignmentDataset, DataEntry

logger = logging.getLogger(__name__)


def sci_hub_pdf(identifier):
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
    res = requests.get(link)
    try:
        pdf_reader = PdfReader(io.BytesIO(res.content))
        return '\n'.join(page.extract_text() for page in pdf_reader.pages)
    except PdfReadError as e:
        logger.error('Could not read PDF file: %s', e)
    return None


def fetch_element(url, selector):
    resp = requests.get(
        url, allow_redirects=True,
        headers={'User-Agent': 'Mozilla /5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0'},
    )
    soup = BeautifulSoup(resp.content, "html.parser")
    return soup.select_one(selector)


def element_extractor(selector, remove=[]):
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
    article = fetch_element(url, 'article')
    article.find('h1').parent.extract()  # remove the header
    return article and MarkdownConverter().convert_soup(article).strip()


def get_doi(doi):
    if link := sci_hub_pdf(doi):
        return extract_pdf(link)
    return None


def doi_getter(url):
    return get_doi(urlparse(url).path.lstrip('/'))


def get_pdf_from_page(link_selector):
    """Get a function that receives an `url` to a page containing a pdf link and returns the pdf's contents as text.

    :param str link_selector: a CSS selector used to find the download link
    :returns: the contents of the pdf file as a string
    """
    def getter(url):
        link = fetch_element(url, link_selector).get('href')
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


PARSERS = {
    'ai.googleblog.com': element_extractor('div.post-body.entry-content'),
    'bair.berkeley.edu': element_extractor('article'),
    'link.springer.com': doi_getter,
    'mediangroup.org': element_extractor('div.entry-content'),
    'www.alexirpan.com': element_extractor('article'),
    'www.incompleteideas.net': element_extractor('body'),
    'ai-alignment.com': medium_blog,
    'aisrp.org': element_extractor('article'),
    'bounded-regret.ghost.io': element_extractor('div.post-content'),
    'casparoesterheld.com': element_extractor('.entry-content', remove=['div.sharedaddy']),
    'cset.georgetown.edu': get_pdf_from_page('a:-soup-contains("Download Full")'),
    'cullenokeefe.com': element_extractor('div.sqs-block-content'),
    'deepmindsafetyresearch.medium.com': medium_blog,
    'docs.google.com': google_doc,
    'docs.microsoft.com': element_extractor('div.content'),
    'doi.org': doi_getter,
    'dl.acm.org': doi_getter,
    'en.wikipedia.org': element_extractor('main.content'),
    'eng.uber.com': element_extractor('div.article-body'),
    'futureoflife.org': element_extractor('div.body-content'),
    'gcrinstitute.org': element_extractor('div.blog-content'),
    'globalprioritiesinstitute.org': get_pdf_from_page('a:-soup-contains("PDF download")'),
    'jbkjr.me': element_extractor('section.page__content'),
    'www.humanityplus.org': element_extractor('div.content'),
    'longtermrisk.org': element_extractor('div.entry-content'),
    'medium.com': medium_blog,
    'openaccess.thecvf.com': get_pdf_from_page('a:-soup-contains("pdf")'),
    'openai.com': element_extractor('#content'),
    'openreview.net': get_pdf_from_page('a.note_content_pdf'),
    'ought.org': element_extractor('div.BlogPostBodyContainer'),
    'papers.nips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'par.nsf.gov': get_pdf_from_page('a:-soup-contains("Accepted Manuscript")'),
    'proceedings.neurips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'psyarxiv.com': get_pdf_from_page('a:-soup-contains("Download")'),
    'rowanzellers.com': get_pdf_from_page('main a:-soup-contains("Paper")'),
    'sideways-view.com': element_extractor('article', remove=['header']),
    'slatestarcodex.com': element_extractor('div.pjgm-postcontent'),
    'thegradient.pub': element_extractor('div.c-content'),
    'towardsdatascience.com': medium_blog,
    'unstableontology.com': element_extractor('.entry-content', remove=['div.sharedaddy']),
    'weightagnostic.github.io': element_extractor('dt-article', remove=['#authors_section', 'dt-byline']),
    'www.cnas.org': element_extractor('#mainbar-toc'),
    'www.econlib.org': element_extractor('div.post-content'),
    'www.gleech.org': element_extractor('article.post-content', remove=['center', 'div.accordion']),
    'www.governance.ai': get_pdf_from_page('a.read-paper-button:not([href="#"])'),
    'www.ibm.com': element_extractor('div:has(> p)'),  # IBM's HTML is really ugly...
    'www.jstor.org': doi_getter,
    'www.microsoft.com': element_extractor('div.content-container'),
    'www.openphilanthropy.org': element_extractor('div.pagenav-content'),
    'www.ri.cmu.edu': get_pdf_from_page('a.pub-link'),
    'www.risksciences.ucla.edu': get_pdf_from_page('a:-soup-contains("Download")'),
    'papers.ssrn.com': get_pdf_from_page('a.button-link.primary'),
    'www.vox.com': element_extractor('div.c-entry-content', remove=['c-article-footer']),
    'www.weforum.org': element_extractor('div.wef-0'),
    'www6.inrae.fr': element_extractor('div.ArticleContent'),
    'yjolt.org': get_pdf_from_page('span.file a'),
    'www.aleph.se': element_extractor('body'),
    'lukemuehlhauser.com': element_extractor('div.entry-content'),
}


def extract_text(url):
    domain = urlparse(url).netloc
    return PARSERS.get(domain, lambda u: None)(url)


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
        if mapped_key := self.mappings.get(key):
            return getattr(item, mapped_key, None)
        return getattr(item, key, None)

    def get_item_key(self, item):
        return self.get_value(item, self.done_key)

    def process_entry(self, item):
        url = self.get_item_key(item)
        print(item.title, url)

        doi = self.get_value(item, 'DOI')
        if url.lower().endswith('pdf'):
            print('getting pdf', url)
            text = extract_pdf(url)
        elif doi and not pd.isna(doi):
            print('getting doi', doi)
            text = get_doi(doi)
        else:
            text = extract_text(url)

        if not text:
            logger.error('Could not process "%s": %s', item.title, item.url)
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
