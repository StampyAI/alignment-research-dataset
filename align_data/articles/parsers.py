import logging
from urllib.parse import urlparse

import regex as re
from markdownify import MarkdownConverter

from align_data.articles.html import fetch, fetch_element, element_extractor
from align_data.articles.pdf import get_pdf_from_page, fetch_pdf, doi_getter

logger = logging.getLogger(__name__)


def google_doc(url: str) -> str:
    """Fetch the contents of the given gdoc url as markdown."""
    res = re.search(r'https://docs.google.com/document/(?:u/)?(?:0/)?d/(.*?)/', url)
    if not res:
        return None

    doc_id = res.group(1)
    body = fetch_element(f'https://docs.google.com/document/d/{doc_id}/export?format=html', 'body')
    if body:
        return  MarkdownConverter().convert_soup(body).strip()


def medium_blog(url):
    """Return the contents of the medium article at the given URL as markdown."""
    article = fetch_element(url, 'article')
    article.find('h1').parent.extract()  # remove the header
    if article:
        return MarkdownConverter().convert_soup(article).strip()


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
    elif content_type & {'text/html'}:
        result['error'] = f'XML is not yet handled'
    else:
        result['error'] = f'unknown content type: {content_type}'

    return result


def error(error_msg):
    """Returns a url handler function that just logs the provided `error` string."""
    def func(url):
        if error_msg:
            logger.error(error_msg)
        return error_msg
    return func


def multistrategy(*funcs):
    """Merges multiple getter functions, returning the result of the first function call to succeed."""
    def getter(url):
        for func in funcs:
            res = func(url)
            if res and 'error' not in res:
                return res
    return getter


UNINPLEMENTED_PARSERS = {
    # Unhandled items that will be caught later. Though it would be good for them also to be done properly
    'oxford.universitypressscholarship.com': error(''),

    # Paywalled journal
    'linkinghub.elsevier.com': error('Elsevier is a known parasite - no point in looking to them for content'),
    'link.springer.com': error('This article looks paywalled'),
    'www.dl.begellhouse.com': error('This article is paywalled'),

    # To be implemented
    'goodreads.com': error('Ebooks are not yet handled'),
    'judiciary.senate.gov': error(''),
    'taylorfrancis.com': error('Ebooks are not yet handled'),
    'YouTube.com': error('Youtube videos are not yet handled'),
    'researchgate.net': error('Researchgate makes it hard to auto download pdf - please provide a DOI or a different url to the contents'),
    'repository.cam.ac.uk': error(''),
}


HTML_PARSERS = {
    'academic.oup.com': element_extractor('#ContentTab'),
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
    'cullenokeefe.com': element_extractor('div.sqs-block-content'),
    'deepmindsafetyresearch.medium.com': medium_blog,
    'docs.google.com': google_doc,
    'docs.microsoft.com': element_extractor('div.content'),
    'digichina.stanford.edu': element_extractor('div.h_editor-content'),
    'en.wikipedia.org': element_extractor('main.mw-body'),
    'eng.uber.com': element_extractor('div.article-body'),
    'futureoflife.org': multistrategy(
        element_extractor('div.body-content'),
        element_extractor('#main-content'),
    ),
    'gcrinstitute.org': element_extractor('div.blog-content'),
    'jbkjr.me': element_extractor('section.page__content'),
    'link.springer.com': element_extractor('article.c-article-body'),
    'longtermrisk.org': element_extractor('div.entry-content'),
    'lukemuehlhauser.com': element_extractor('div.entry-content'),
    'medium.com': medium_blog,
    'openai.com': element_extractor('#content'),
    'ought.org': element_extractor('div.BlogPostBodyContainer'),
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
    'ibm.com': element_extractor('div:has(> p)'),  # IBM's HTML is really ugly...
    'microsoft.com': element_extractor('div.content-container'),
    'mdpi.com': element_extractor(
        'article', remove=[
            '.article-icons', '.title', '.art-authors', '.art-affiliations', '.bib-identity',
            '.pubhistory', '.belongsTo', '.highlight-box1', '.additional-content'
        ]
    ),
    'nature.com': element_extractor('article', remove=['header', '#rights link-section', '#article-info-section']),
    'ncbi.nlm.nih.gov': element_extractor('div.article'),
    'openphilanthropy.org': element_extractor('div.pagenav-content'),
    'safe.ai': element_extractor('#open-letter'),
    'sciencedirect.com': element_extractor(
        'article',
        remove=[
            '#section-cited-by', '.Copyright', '.issue-navigation', '.ReferencedArticles',
            '.LicenseInfo', '.ArticleIdentifierLinks', '.Banner', '.screen-reader-main-title', '.Publication'
        ]
    ),
    'vox.com': element_extractor('did.c-entry-content', remove=['c-article-footer']),
    'weforum.org': element_extractor('div.wef-0'),
    'www6.inrae.fr': element_extractor('div.ArticleContent'),
    'aleph.se': element_extractor('body'),
    'yoshuabengio.org': element_extractor('div.post-content'),
}

PDF_PARSERS = {
    # Domain sepecific handlers
    'apcz.umk.pl': get_pdf_from_page('.galleys_links a.pdf', 'a.download'),
    'academic.oup.com': get_pdf_from_page('a.article-pdfLink'),
    'cset.georgetown.edu': get_pdf_from_page('a:-soup-contains("Download Full")'),
    'drive.google.com': extract_gdrive_contents,
    'doi.org': doi_getter,
    'dl.acm.org': fetch_pdf,
    'dspace.mit.edu': get_pdf_from_page('a.btn-primary.download-button'),
    'globalprioritiesinstitute.org': get_pdf_from_page('a:-soup-contains("PDF")'),
    'link.springer.com': multistrategy(
        get_pdf_from_page('div.c-pdf-download a'),
        doi_getter,
    ),
    'openaccess.thecvf.com': get_pdf_from_page('a:-soup-contains("pdf")'),
    'openreview.net': get_pdf_from_page('a.note_content_pdf'),
    'ora.ox.ac.uk': fetch_pdf,
    'papers.nips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'papers.ssrn.com': get_pdf_from_page('.abstract-buttons a.button-link:-soup-contains("Download")'),
    'par.nsf.gov': get_pdf_from_page('a:-soup-contains("Accepted Manuscript")'),
    'proceedings.neurips.cc': get_pdf_from_page('a:-soup-contains("Paper")'),
    'psyarxiv.com': lambda url: fetch_pdf(url.rstrip('/') + '/download'),
    'rowanzellers.com': get_pdf_from_page('main a:-soup-contains("Paper")'),
    'governance.ai': get_pdf_from_page('a.read-paper-button:not([href="#"])'),
    'ijcai.org': get_pdf_from_page('a.btn-download:-soup-contains("PDF")'),
    'jair.org': get_pdf_from_page('div.download a.pdf', 'a.download'),
    'jstor.org': doi_getter,
    'ri.cmu.edu': get_pdf_from_page('a.pub-link'),
    'risksciences.ucla.edu': get_pdf_from_page('a:-soup-contains("Download")'),
    'ssrn.com': get_pdf_from_page('.abstract-buttons a.button-link:-soup-contains("Download")'),
    'yjolt.org': get_pdf_from_page('span.file a'),
}


def item_metadata(url):
    domain = urlparse(url).netloc.lstrip('www.')
    res = fetch(url, 'head')
    content_type = {item.strip() for item in res.headers.get('Content-Type').split(';')}

    if content_type & {'text/html', 'text/xml'}:
        # If the url points to a html webpage, then it either contains the text as html, or
        # there is a link to a pdf on it
        if parser := HTML_PARSERS.get(domain):
            if res := parser(url):
                # Proper contents were found on the page, so use them
                return {'source_url': url, 'data_source': 'html'}

        if parser := PDF_PARSERS.get(domain):
            if res := parser(url):
                # A pdf was found - use it, though it might not be useable
                return res

        if parser := UNINPLEMENTED_PARSERS.get(domain):
            return {'error': parser(url)}

        if domain not in (HTML_PARSERS.keys() | PDF_PARSERS.keys() | UNINPLEMENTED_PARSERS.keys()):
            return {'error': 'No domain handler defined'}
    elif content_type & {'application/octet-stream', 'application/pdf'}:
        # this looks like it could be a pdf - try to download it as one
        return fetch_pdf(url)
    elif content_type & {'application/epub+zip', 'application/epub'}:
        # it looks like an ebook. Assume it's fine.
        # TODO: validate that the ebook is readable
        return {'source_url': url, 'data_source': 'ebook'}
    else:
        return {'error': f'Unhandled content type: {content_type}'}
