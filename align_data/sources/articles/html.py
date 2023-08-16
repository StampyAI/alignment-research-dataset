import time
import logging
from typing import Union

import requests
from bs4 import BeautifulSoup, Tag
from markdownify import MarkdownConverter

import logging
logger = logging.getLogger(__name__)


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/113.0",
}


def with_retry(times=3, exceptions=requests.exceptions.RequestException):
    """A decorator that will retry the wrapped function up to `times` times in case of google sheets errors."""

    def wrapper(f):
        def retrier(*args, **kwargs):
            for i in range(times):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    logger.error(f"{e} - retrying up to {times - i} times")
                    # Do a logarithmic backoff
                    time.sleep((i + 1) ** 2)
            raise ValueError(f"Gave up after {times} tries")

        return retrier

    return wrapper


def fetch(url, method="get", headers=DEFAULT_HEADERS):
    """Fetch the given `url`.

    This function is to have a single place to manage headers etc.
    """
    return getattr(requests, method)(url, allow_redirects=True, headers=headers)


def fetch_element(url: str, selector: str, headers=DEFAULT_HEADERS) -> Union[Tag, None]:
    """Fetch the first HTML element that matches the given CSS `selector` on the page found at `url`."""
    try:
        resp = fetch(url, headers=headers)
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to %s", url)
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
