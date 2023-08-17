import logging
from urllib.parse import urlparse, urljoin
from typing import Dict, Optional, Callable, Any

from markdownify import MarkdownConverter
from requests.exceptions import ConnectionError, InvalidSchema, MissingSchema

from align_data.sources.articles.html import element_extractor, fetch, fetch_element
from align_data.sources.articles.pdf import doi_getter, fetch_pdf, get_arxiv_pdf, parse_vanity
from align_data.sources.articles.google_cloud import google_doc, extract_gdrive_contents

logger = logging.getLogger(__name__)

HtmlParserFunc = Callable[[str], str | None]
PdfParserFunc = Callable[[str], Dict[str, Any]]
UnimplementedParserFunc = Callable[[str], Optional[str | None]]


def get_pdf_from_page(*link_selectors: str):
    """Get a function that receives an `url` to a page containing a pdf link and returns the pdf's contents as text.

    Starting from `url`, fetch the contents at the URL, extract the link using a CSS selector, then:
     * if there are more selectors left, fetch the contents at the extracted link and continue
     * otherwise return the pdf contents at the last URL

    :param str *link_selectors: CSS selectors used to find the final download link
    :returns: the contents of the pdf file as a string
    """
    def getter(url: str) -> Dict[str, Any]:
        current_url: str = url

        for selector in link_selectors:
            elem = fetch_element(current_url, selector)
            if not elem:
                return {"error": f"Could not find pdf download link for {current_url} using '{selector}'"}

            # Extracting href, considering it can be a string or a list of strings
            href = elem.get("href")
            if isinstance(href, list):
                href = href[0] if href else None

            if not href:
                return {"error": f"Could not extract href for {current_url} using '{selector}'"}

            # Making sure the link is absolute
            if not href.startswith(("http", "//")):
                href = urljoin(url, href)

            current_url = href
        # Some pages keep link to google drive previews of pdf files, which need to be
        # mangled to get the URL of the actual pdf file
        if "drive.google.com" in current_url and "/view" in current_url:
            return extract_gdrive_contents(current_url)

        if pdf := fetch_pdf(current_url):
            return pdf
        return {"error": f"Could not fetch pdf from {current_url}"}
    return getter


def medium_blog(url: str) -> str | None:
    """Return the contents of the medium article at the given URL as markdown."""
    # Medium does some magic redirects if it detects that the request is from firefox
    article = fetch_element(url, "article", headers=None)
    if not article:
        return None

    # remove the header
    title = article.find("h1")
    if title and title.parent:
        title.parent.extract()

    return MarkdownConverter().convert_soup(article).strip()


def error(error_msg: str) -> UnimplementedParserFunc:
    """Returns a url handler function that just logs the provided `error` string."""
    def func(_url) -> str | None:
        if error_msg:
            logger.error(error_msg)
        return error_msg

    return func


def multistrategy(*funcs):
    """Merges multiple getter functions, returning the result of the first function call to succeed."""
    """This works for HtmlParsersFuncs or PdfParserFuncs."""

    def getter(url: str):
        for func in funcs:
            res = func(url)
            if res and "error" not in res:
                return res

    return getter


UNIMPLEMENTED_PARSERS: Dict[str, UnimplementedParserFunc] = {
    # Unhandled items that will be caught later. Though it would be good for them also to be done properly
    "oxford.universitypressscholarship.com": error(""),
    # Paywalled journal
    "linkinghub.elsevier.com": error(
        "Elsevier is a known parasite - no point in looking to them for content"
    ),
    "link.springer.com": error("This article looks paywalled"),
    "www.dl.begellhouse.com": error("This article is paywalled"),
    "dl.begellhouse.com": error("Begell house is not yet handled"),

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
    
    "deliverypdf.ssrn.com": error("SSRN is not yet handled"),
    "doi.wiley.com": error("Wiley is not yet handled"),
    "onlinelibrary.wiley.com": error("Wiley is not yet handled"),
    "globalprioritiesproject.org": error("Global priorities project is not yet handled"),
    "ieeexplore.ieee.org": error("IEEE is not yet handled"),
    "pdcnet.org": error("pdcnet.org is not yet handled"),
    "sciencemag.org": error("sciencemag.org is not yet handled"),
    "iopscience.iop.org": error("iopscience.iop.org is not yet handled"),
    "journals.aom.org": error("journals.aom.org is not yet handled"),
    "cambridge.org": error("cambridge.org is not yet handled"),
    "transformer-circuits.pub": error("not handled yet - same codebase as distill"),

}


HTML_PARSERS: Dict[str, HtmlParserFunc] = {
    "academic.oup.com": element_extractor("#ContentTab"),
    "ai.googleblog.com": element_extractor("div.post-body.entry-content"),
    # TODO: arxiv-vanity.com does not output the same type as the other parsers: Dict[str, str] instead of str
    # ar5iv.labs.arxiv.org too. Are these pdf parsers? not rly, but they don't output the same type as the other html parsers
    #"arxiv-vanity.com": parse_vanity,
    #"ar5iv.labs.arxiv.org": parse_vanity,
    "bair.berkeley.edu": element_extractor("article"),
    "mediangroup.org": element_extractor("div.entry-content"),
    "www.alexirpan.com": element_extractor("article"),
    "www.incompleteideas.net": element_extractor("body"),
    "ai-alignment.com": medium_blog,
    "aisrp.org": element_extractor("article"),
    "bounded-regret.ghost.io": element_extractor("div.post-content"),
    "carnegieendowment.org": element_extractor(
        "div.article-body", remove=[".no-print", ".related-pubs"]
    ),
    "casparoesterheld.com": element_extractor(
        ".entry-content", remove=["div.sharedaddy"]
    ),
    "cullenokeefe.com": element_extractor("div.sqs-block-content"),
    "deepmindsafetyresearch.medium.com": medium_blog,
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
    "medium.com": medium_blog,
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
    "towardsdatascience.com": medium_blog,
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
    "vox.com": element_extractor("did.c-entry-content", remove=["c-article-footer"]),
    "weforum.org": element_extractor("div.wef-0"),
    "www6.inrae.fr": element_extractor("div.ArticleContent"),
    "aleph.se": element_extractor("body"),
    "yoshuabengio.org": element_extractor("div.post-content"),
}

PDF_PARSERS: Dict[str, PdfParserFunc] = {
    # Domain sepecific handlers
    "apcz.umk.pl": get_pdf_from_page(".galleys_links a.pdf", "a.download"),
    "arxiv.org": get_arxiv_pdf,
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
    net_loc = urlparse(url).netloc
    return net_loc[4:] if net_loc.startswith("www.") else net_loc


def item_metadata(url: str) -> Dict[str, Any]: 
    if not url:
        return {"error": "No url was given to item_metadata"}
    domain = parse_domain(url)
    try:
        res = fetch(url, "head")
    except (MissingSchema, InvalidSchema, ConnectionError) as e:
        return {"error": str(e)}
    
    if not res.headers.get('Content-Type'):
        return {'error': 'No content type found'}

    content_type = {item.strip() for item in res.headers.get("Content-Type", "").split(";")}

    if content_type & {"text/html", "text/xml"}:
        # If the url points to a html webpage, then it either contains the text as html, or
        # there is a link to a pdf on it
        if parser := HTML_PARSERS.get(domain):
            if parsed_html := parser(url):
                #TODO: For some HTML_PARSERS like parse_vanity, it outputs 
                # a dict of metadata instead of a string. This should be fixed or changed
                
                # Proper contents were found on the page, so use them
                return {"source_url": url, "source_type": "html", "text": parsed_html}

        if parser := PDF_PARSERS.get(domain):
            if content := parser(url):
                # A pdf was found - use it, though it might not be useable
                return content

        if parser := UNIMPLEMENTED_PARSERS.get(domain):
            return {"error": parser(url)}

        if domain not in (
            HTML_PARSERS.keys() | PDF_PARSERS.keys() | UNIMPLEMENTED_PARSERS.keys()
        ):
            return {"error": f"No domain handler defined for {domain}"}
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
