import re
from typing import Any, Dict, List

from align_data.settings import ARTICLE_MAIN_KEYS
from align_data.sources.utils import merge_dicts


def normalize_url(url: str | None) -> str | None:
    if not url:
        return url

    # ending '/'
    url = url.rstrip("/")

    # Remove http and use https consistently
    url = url.replace("http://", "https://")

    # Remove www
    url = url.replace("https://www.", "https://")

    # Remove index.html or index.htm
    url = re.sub(r'/index\.html?$', '', url)

    # Convert youtu.be links to youtube.com
    url = url.replace("https://youtu.be/", "https://youtube.com/watch?v=")

    # Additional rules for mirror domains can be added here

    # agisafetyfundamentals.com -> aisafetyfundamentals.com
    url = url.replace("https://agisafetyfundamentals.com", "https://aisafetyfundamentals.com")

    return url


def normalize_text(text: str | None) -> str | None:
    return (text or '').replace('\n', ' ').replace('\r', '').strip() or None


def format_authors(authors: List[str]) -> str:
    # TODO: Don't keep adding the same authors - come up with some way to reuse them
    authors_str = ",".join(authors)
    if len(authors_str) > 1024:
        authors_str = ",".join(authors_str[:1024].split(",")[:-1])
    return authors_str


def article_dict(data, **kwargs) -> Dict[str, Any]:
    data = merge_dicts(data, kwargs)

    summaries = data.pop("summaries", [])
    summary = data.pop("summary", None)

    # Preserve explicit summaries even when no single `summary` field is provided
    data['summaries'] = summaries + [summary] if summary else summaries
    data['authors'] = format_authors(data.pop("authors", []))
    data['title'] = normalize_text(data.get('title'))

    return dict(
        meta={k: v for k, v in data.items() if k not in ARTICLE_MAIN_KEYS and v is not None},
        **{k: v for k, v in data.items() if k in ARTICLE_MAIN_KEYS},
    )
