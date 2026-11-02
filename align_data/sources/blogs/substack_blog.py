import logging
import re
from urllib.parse import urlparse

import requests

from align_data.common.html_dataset import RSSDataset
from align_data.common.retry import with_retry

logger = logging.getLogger(__name__)

_ARCHIVE_PAGE_SIZE = 20
# The RSS feed is the latest ~20 posts. Two archive pages cover that window.
_ARCHIVE_PAGE_CAP = 2
# Project writeups often have no dc:creator; the byline is in the first paragraph.
_SUBMITTED_BY = re.compile(
    r"submitted by\s+(?:<a\b[^>]*>)?\s*([^<.]+)",
    re.IGNORECASE,
)


def _submitted_by_author(item) -> list[str]:
    html = ""
    content = item.get("content")
    if isinstance(content, list) and content and isinstance(content[0], dict):
        html = content[0].get("value") or ""
    if not html:
        html = item.get("description") or item.get("summary") or ""
    match = _SUBMITTED_BY.search(html[:1000])
    if not match:
        return []
    name = re.sub(r"\s+", " ", match.group(1)).strip(" \t\r\n\"'")
    if not name or len(name) > 80:
        return []
    return [name]


def _normalize_post_url(url: str) -> str:
    parsed = urlparse(url or "")
    if not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"


@with_retry(times=3, exceptions=(requests.Timeout, requests.ConnectionError))
def _get_archive_page(url: str) -> list:
    resp = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; alignment-research-dataset)"},
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected archive payload from {url}")
    return data


class SubstackBlog(RSSDataset):
    source_type = "substack"
    date_format = "%a, %d %b %Y %H:%M:%S %Z"

    @property
    def feed_url(self):
        return self.url + "/feed"

    @property
    def items_list(self):
        urls = super().items_list
        # blog.bluedot.org's RSS dc:creator is only the primary author.
        if urlparse(self.url).hostname == "blog.bluedot.org":
            self._attach_published_bylines()
        return urls

    def extract_authors(self, item):
        bylines = item.get("published_bylines")
        if bylines:
            return [name.strip() for name in bylines if name and name.strip()]
        authors = super().extract_authors(item)
        if authors or urlparse(self.url).hostname != "blog.bluedot.org":
            return authors
        return _submitted_by_author(item) or authors

    def _attach_published_bylines(self) -> None:
        """Copy every published byline onto the feed item.

        Substack's RSS feed lists one ``dc:creator``. The archive API lists
        every author. If that request fails, the RSS author is kept.
        """
        bylines = self._fetch_archive_bylines()
        for url, item in self.items.items():
            names = bylines.get(_normalize_post_url(url))
            if names:
                item["published_bylines"] = names

    def _fetch_archive_bylines(self) -> dict[str, list[str]]:
        wanted = {_normalize_post_url(url) for url in self.items}
        found: dict[str, list[str]] = {}
        offset = 0
        base = self.url.rstrip("/")
        pages = 0
        while wanted - found.keys():
            archive_url = (
                f"{base}/api/v1/archive?sort=new&offset={offset}&limit={_ARCHIVE_PAGE_SIZE}"
            )
            try:
                posts = _get_archive_page(archive_url)
            except (requests.RequestException, ValueError) as err:
                logger.warning(
                    "Could not fetch BlueDot bylines from %s: %s", archive_url, err
                )
                break
            if not posts:
                break
            added = False
            for post in posts:
                canonical = post.get("canonical_url")
                if not canonical and post.get("slug"):
                    canonical = f"{base}/p/{post['slug']}"
                key = _normalize_post_url(canonical or "")
                names = [
                    name
                    for byline in (post.get("publishedBylines") or [])
                    if isinstance(byline, dict)
                    and (name := (byline.get("name") or "").strip())
                ]
                if key in wanted and names and key not in found:
                    found[key] = names
                    added = True
            offset += len(posts)
            pages += 1
            if not added or len(posts) < _ARCHIVE_PAGE_SIZE or pages >= _ARCHIVE_PAGE_CAP:
                break
        return found
