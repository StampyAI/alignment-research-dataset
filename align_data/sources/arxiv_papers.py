import logging
import re
from typing import Dict, Optional, Any

import arxiv

from align_data.sources.articles.pdf import fetch_pdf, parse_vanity
from align_data.sources.articles.html import fetch_element
from align_data.sources.utils import merge_dicts

logger = logging.getLogger(__name__)


def get_arxiv_metadata(paper_id: str) -> arxiv.Result | None:
    """
    Get metadata from arxiv
    """
    try:
        search = arxiv.Search(id_list=[paper_id], max_results=1)
        return next(search.results())
    except Exception as e:
        logger.error(e)
    return None


def get_id(url: str) -> str | None:
    if res := re.search(r"https?://arxiv.org/(?:abs|pdf)/(.*?)(?:v\d+)?(?:/|\.pdf)?$", url):
        return res.group(1)
    return None


def canonical_url(url: str) -> str:
    if paper_id := get_id(url):
        return f"https://arxiv.org/abs/{paper_id}"
    return url


def get_contents(paper_id: str) -> Dict[str, Any]:
    arxiv_vanity = parse_vanity(f"https://www.arxiv-vanity.com/papers/{paper_id}")
    if "error" not in arxiv_vanity:
        return arxiv_vanity

    ar5iv = parse_vanity(f"https://ar5iv.org/abs/{paper_id}")
    if "error" not in ar5iv:
        return ar5iv

    return fetch_pdf(f"https://arxiv.org/pdf/{paper_id}.pdf")


def get_version(id: str) -> str | None:
    if res := re.search(r".*v(\d+)$", id):
        return res.group(1)


def is_withdrawn(url: str) -> bool:
    if elem := fetch_element(canonical_url(url), '.extra-services .full-text ul'):
        return elem.text.strip().lower() == 'withdrawn'
    return False


def add_metadata(data: Dict[str, Any], paper_id: str) -> Dict[str, Any]:
    metadata = get_arxiv_metadata(paper_id)
    if not metadata:
        return {}
    return merge_dicts(
        {
            "authors": metadata.authors,
            "title": metadata.title,
            "date_published": metadata.published,
            "data_last_modified": metadata.updated.isoformat(),
            "summary": metadata.summary.replace("\n", " "),
            "comment": metadata.comment,
            "journal_ref": metadata.journal_ref,
            "doi": metadata.doi,
            "primary_category": metadata.primary_category,
            "categories": metadata.categories,
            "version": get_version(metadata.get_short_id()),
        },
        data,
    )


def fetch_arxiv(url: str) -> Dict[str, Any]:
    paper_id = get_id(url)
    if not paper_id:
        return {"error": "Could not extract arxiv id"}

    if is_withdrawn(url):
        return {
            "url": canonical_url(url),
            "status": "Withdrawn",
            "text": None,
            "source_type": None
        }

    paper = get_contents(paper_id)
    data = add_metadata(
        {
            "url": canonical_url(url),
            "source_type": paper.get("data_source"),
        },
        paper_id,
    )
    authors = data.get("authors") or paper.get("authors") or []
    data["authors"] = [str(a).strip() for a in authors]
    data["source"] = "arxiv"

    return merge_dicts(data, paper)
