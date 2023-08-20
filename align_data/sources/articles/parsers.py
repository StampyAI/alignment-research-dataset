import logging
from urllib.parse import urlparse, urljoin
from typing import Dict

from requests.exceptions import ConnectionError, InvalidSchema, MissingSchema

from align_data.sources.articles.html import element_extractor, fetch, fetch_element
from align_data.sources.articles.pdf import doi_getter, fetch_pdf, parse_vanity
from align_data.sources.articles.google_cloud import google_doc, extract_gdrive_contents
from align_data.sources.arxiv_papers import fetch_arxiv
from align_data.common.html_dataset import HTMLDataset


logger = logging.getLogger(__name__)


def get_pdf_from_page(*link_selectors: str):
    """Get a function that receives an `url` to a page containing a pdf link and returns the pdf's contents as text.

    Starting from `url`, fetch the contents at the URL, extract the link using a CSS selector, then:
     * if there are more selectors left, fetch the contents at the extracted link and continue
     * otherwise return the pdf contents at the last URL

    :param List[str] link_selectors: CSS selector used to find the final download link
    :returns: the contents of the pdf file as a string
    """
    def getter(url: str):
        link: str = url
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

        if parse_domain(link) == "arxiv.org":
            return fetch_arxiv(link)
        if pdf := fetch_pdf(link):
            return pdf
        return {'error': f'Could not fetch pdf from {link}'}
    return getter


class MediumParser(HTMLDataset):
    """
    Fetches articles from a Medium blog.

    Pulls Medium articles by walking the archive. Depending on the activity of the blog
    during a particular year, the archive for the year may consist of a single page only, or
    may have daily pages. A single blog can use different layouts for different years.

    Also, if the blog had few posts overall, an archive may not exist at all. In that case,
    the main page is used to fetch the articles. The entries are assumed to fit onto
    a single page, which seems to be the case for blogs without an archive.

    It is possible that there is additional variation in the layout that hasn't been represented
    in the blogs tested so far. In that case, additional fixes to this code may be needed.
    """

    source_type = "MediumParser(name='html', url='')"
    ignored_selectors = ["div:first-child span"]

    def _get_published_date(self, contents):
        possible_date_elements = contents.select("article div:first-child span")
        return self._find_date(possible_date_elements)

    def __call__(self, url):
        return self.get_contents(url)


def error(error_msg):
    """Returns a url handler function that just logs the provided `error` string."""
    def func(url):
        if error_msg:
            logger.error(error_msg)
        return {'error': error_msg, 'source_url': url}

    return func


def multistrategy(*funcs):
    """Merges multiple getter functions, returning the result of the first function call to succeed."""

    def getter(url):
        for func in funcs:
            res = func(url)
            if res and "error" not in res:
                return res

    return getter


UNIMPLEMENTED_PARSERS = {
    # Unhandled items that will be caught later. Though it would be good for them also to be done properly
    "oxford.universitypressscholarship.com": error(""),
    # Paywalled journal
    "linkinghub.elsevier.com": error(
        "Elsevier is a known parasite - no point in looking to them for content"
    ),
    "link.springer.com": error("This article looks paywalled"),
    "www.dl.begellhouse.com": error("This article is paywalled"),
    # To be implemented
    "goodreads.com": error("Ebooks are not yet handled"),
    "judiciary.senate.gov": error(""),
    "taylorfrancis.com": error("Ebooks are not yet handled"),
    "YouTube.com": error("Youtube videos are not yet handled"),
    "youtube.com": error("Youtube videos are not yet handled"),
    "YouTube.be": error("Youtube videos are not yet handled"),
    "researchgate.net": error(
        "Researchgate makes it hard to auto download pdf - please provide a DOI or a different url to the contents"
    ),
    "repository.cam.ac.uk": error(""),
}


HTML_PARSERS = {
    "academic.oup.com": element_extractor("#ContentTab"),
    "ai.googleblog.com": element_extractor("div.post-body.entry-content"),
    "arxiv-vanity.com": parse_vanity,
    "ar5iv.labs.arxiv.org": parse_vanity,
    "bair.berkeley.edu": element_extractor("article"),
    "mediangroup.org": element_extractor("div.entry-content"),
    "www.alexirpan.com": element_extractor("article"),
    "www.incompleteideas.net": element_extractor("body"),
    "ai-alignment.com": MediumParser(name='html', url='ai-alignment.com'),
    "aisrp.org": element_extractor("article"),
    "bounded-regret.ghost.io": element_extractor("div.post-content"),
    "carnegieendowment.org": element_extractor(
        "div.article-body", remove=[".no-print", ".related-pubs"]
    ),
    "casparoesterheld.com": element_extractor(
        ".entry-content", remove=["div.sharedaddy"]
    ),
    "cullenokeefe.com": element_extractor("div.sqs-block-content"),
    "deepmindsafetyresearch.medium.com": MediumParser(name='html', url='deepmindsafetyresearch.medium.com'),
    "docs.google.com": google_doc,
    "docs.microsoft.com": element_extractor("div.content"),
    "digichina.stanford.edu": element_extractor("div.h_editor-content"),
    "en.wikipedia.org": element_extractor("main.mw-body"),
    "eng.uber.com": element_extractor("div.article-body"),
    "futureoflife.org": multistrategy(
        element_extractor("div.body-content"),
        element_extractor("#main-content"),
    ),
    "gcrinstitute.org": element_extractor("div.blog-content"),
    "jbkjr.me": element_extractor("section.page__content"),
    "link.springer.com": element_extractor("article.c-article-body"),
    "longtermrisk.org": element_extractor("div.entry-content"),
    "lukemuehlhauser.com": element_extractor("div.entry-content"),
    "medium.com": MediumParser(name='html', url='medium.com'),
    "openai.com": element_extractor("#content"),
    "ought.org": element_extractor("div.BlogPostBodyContainer"),
    "sideways-view.com": element_extractor("article", remove=["header"]),
    "slatestarcodex.com": element_extractor("div.pjgm-postcontent"),
    "techpolicy.press": element_extractor(
        "div.post-content",
        remove=[
            "div.before_content",
            ".sabox-guest-authors-container",
            ".jp-relatedposts",
        ],
    ),
    "theconversation.com": element_extractor("div.content-body"),
    "thegradient.pub": element_extractor("div.c-content"),
    "towardsdatascience.com": MediumParser(name='html', url='towardsdatascience.com'),
    "unstableontology.com": element_extractor(
        ".entry-content", remove=["div.sharedaddy"]
    ),
    "waitbutwhy.com": element_extractor("article", remove=[".entry-header"]),
    "weightagnostic.github.io": element_extractor(
        "dt-article", remove=["#authors_section", "dt-byline"]
    ),
    "cnas.org": element_extractor("#mainbar-toc"),
    "econlib.org": element_extractor("div.post-content"),
    "humanityplus.org": element_extractor("div.content"),
    "gleech.org": element_extractor(
        "article.post-content", remove=["center", "div.accordion"]
    ),
    "ibm.com": element_extractor("div:has(> p)"),  # IBM's HTML is really ugly...
    "microsoft.com": element_extractor("div.content-container"),
    "mdpi.com": element_extractor(
        "article",
        remove=[
            ".article-icons",
            ".title",
            ".art-authors",
            ".art-affiliations",
            ".bib-identity",
            ".pubhistory",
            ".belongsTo",
            ".highlight-box1",
            ".additional-content",
        ],
    ),
    "nature.com": element_extractor(
        "article", remove=["header", "#rights link-section", "#article-info-section"]
    ),
    "ncbi.nlm.nih.gov": element_extractor("div.article"),
    "openphilanthropy.org": element_extractor("div.pagenav-content"),
    "safe.ai": element_extractor("#open-letter"),
    "sciencedirect.com": element_extractor(
        "article",
        remove=[
            "#section-cited-by",
            ".Copyright",
            ".issue-navigation",
            ".ReferencedArticles",
            ".LicenseInfo",
            ".ArticleIdentifierLinks",
            ".Banner",
            ".screen-reader-main-title",
            ".Publication",
        ],
    ),
    "transformer-circuits.pub": error("not handled yet - same codebase as distill"),
    "vox.com": element_extractor("did.c-entry-content", remove=["c-article-footer"]),
    "weforum.org": element_extractor("div.wef-0"),
    "www6.inrae.fr": element_extractor("div.ArticleContent"),
    "aleph.se": element_extractor("body"),
    "yoshuabengio.org": element_extractor("div.post-content"),
}

PDF_PARSERS = {
    # Domain sepecific handlers
    "apcz.umk.pl": get_pdf_from_page(".galleys_links a.pdf", "a.download"),
    "arxiv.org": fetch_arxiv,
    "academic.oup.com": get_pdf_from_page("a.article-pdfLink"),
    "cset.georgetown.edu": get_pdf_from_page('a:-soup-contains("Download Full")'),
    "drive.google.com": extract_gdrive_contents,
    "doi.org": doi_getter,
    "dl.acm.org": fetch_pdf,
    "dspace.mit.edu": get_pdf_from_page("a.btn-primary.download-button"),
    "globalprioritiesinstitute.org": get_pdf_from_page('a:-soup-contains("PDF")'),
    "link.springer.com": multistrategy(
        get_pdf_from_page("div.c-pdf-download a"),
        doi_getter,
    ),
    "openaccess.thecvf.com": get_pdf_from_page('a:-soup-contains("pdf")'),
    "openreview.net": get_pdf_from_page("a.note_content_pdf"),
    "ora.ox.ac.uk": fetch_pdf,
    "papers.nips.cc": get_pdf_from_page('a:-soup-contains("Paper")'),
    "papers.ssrn.com": get_pdf_from_page(
        '.abstract-buttons a.button-link:-soup-contains("Download")'
    ),
    "par.nsf.gov": get_pdf_from_page('a:-soup-contains("Accepted Manuscript")'),
    "proceedings.neurips.cc": get_pdf_from_page('a:-soup-contains("Paper")'),
    "psyarxiv.com": lambda url: fetch_pdf(url.rstrip("/") + "/download"),
    "rowanzellers.com": get_pdf_from_page('main a:-soup-contains("Paper")'),
    "governance.ai": get_pdf_from_page('a.read-paper-button:not([href="#"])'),
    "ijcai.org": get_pdf_from_page('a.btn-download:-soup-contains("PDF")'),
    "jair.org": get_pdf_from_page("div.download a.pdf", "a.download"),
    "jstor.org": doi_getter,
    "ri.cmu.edu": get_pdf_from_page("a.pub-link"),
    "risksciences.ucla.edu": get_pdf_from_page('a:-soup-contains("Download")'),
    "ssrn.com": get_pdf_from_page(
        '.abstract-buttons a.button-link:-soup-contains("Download")'
    ),
    "yjolt.org": get_pdf_from_page("span.file a"),
}


def parse_domain(url: str) -> str:
    return url and urlparse(url).netloc.lstrip('www.')


def item_metadata(url) -> Dict[str, any]:
    domain = parse_domain(url)
    try:
        res = fetch(url, 'head')
    except (MissingSchema, InvalidSchema, ConnectionError) as e:
        return {'error': str(e)}

    content_type = {item.strip() for item in res.headers.get('Content-Type', '').split(';')}

    if content_type & {"text/html", "text/xml"}:
        # If the url points to a html webpage, then it either contains the text as html, or
        # there is a link to a pdf on it
        if parser := HTML_PARSERS.get(domain):
            res = parser(url)
            if res and 'error' not in res:
                # Proper contents were found on the page, so use them
                return res

        if parser := PDF_PARSERS.get(domain):
            if res := parser(url):
                # A pdf was found - use it, though it might not be useable
                return res

        if parser := UNIMPLEMENTED_PARSERS.get(domain):
            return parser(url)

        if domain not in (
            HTML_PARSERS.keys() | PDF_PARSERS.keys() | UNIMPLEMENTED_PARSERS.keys()
        ):
            return {"error": "No domain handler defined"}
        return {"error": "could not parse url"}
    elif content_type & {"application/octet-stream", "application/pdf"}:
        # this looks like it could be a pdf - try to download it as one
        return fetch_pdf(url)
    elif content_type & {"application/epub+zip", "application/epub"}:
        # it looks like an ebook. Assume it's fine.
        # TODO: validate that the ebook is readable
        return {"source_url": url, "source_type": "ebook"}
    else:
        return {"error": f"Unhandled content type: {content_type}"}
