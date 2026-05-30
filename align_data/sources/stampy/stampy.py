import sys
import re
import time
import logging
import requests
from dataclasses import dataclass

import html

from align_data.common.alignment_dataset import AlignmentDataset
from align_data.settings import CODA_TOKEN, CODA_DOC_ID, ON_SITE_TABLE

logger = logging.getLogger(__name__)


headers = {"Authorization": f"Bearer {CODA_TOKEN}"}


# Transient network failures worth retrying. ReadTimeout (the 2026-05-30 daily
# failure: host='coda.io', read timeout=30) is a Timeout subclass.
CODA_TRANSIENT_ERRORS = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)


def _coda_get(url, n=3):
    """GET a Coda API url and return its parsed JSON, retrying transient blips.

    Only network timeouts/connection errors are retried (exponential backoff);
    HTTP-status and schema errors raise immediately since they won't self-resolve.
    On exhaustion the last timeout propagates -- callers (get_rows) materialize the
    whole list before any DB write, so a failure here aborts cleanly with no partial
    write rather than half-updating the table.
    """
    last_error = None
    for i in range(n):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            break
        except CODA_TRANSIENT_ERRORS as e:
            last_error = e
            logger.warning(f"Coda request to {url} failed (attempt {i + 1}/{n}): {e}")
            time.sleep(2 ** i)
    else:
        raise last_error

    if response.status_code != 200:
        logger.error(f"Request to {url} failed with status {response.status_code}")
        logger.error(f"Response: {response.text[:1000]}")
        raise Exception(f"Failed to fetch {url}: {response.status_code} - {response.text[:200]}")

    try:
        data = response.json()
    except Exception as e:
        logger.error(f"Failed to parse JSON response from {url}: {response.text[:1000]}")
        raise Exception(f"Failed to parse JSON from {url}: {e}")

    if "items" not in data:
        raise Exception(f"Response from {url} missing 'items' field: {data}")

    return data


def get_columns():
    uri = f"https://coda.io/apis/v1/docs/{CODA_DOC_ID}/tables/{ON_SITE_TABLE}/columns"
    logger.info(f"Fetching columns from {uri}")
    return {c["id"]: c["name"] for c in _coda_get(uri)["items"]}


def paginated(url):
    logger.info(f"Fetching page from {url}")
    resp = _coda_get(url)
    yield from resp["items"]
    if more := resp.get("nextPageLink"):
        yield from paginated(more)


def format_row(row, columns: dict[str, str]) -> dict[str, str]:
    values = row.pop("values")
    return row | {
        column_name: v for k, v in values.items() if (column_name := columns.get(k))
    }


def get_rows(columns: dict[str, str]):
    return [
        format_row(row, columns)
        for row in paginated(
            f"https://coda.io/apis/v1/docs/{CODA_DOC_ID}/tables/{ON_SITE_TABLE}/rows"
        )
    ]


@dataclass
class Stampy(AlignmentDataset):
    done_key = "title"

    def setup(self):
        if not CODA_TOKEN:
            print(
                f"No CODA_TOKEN found! Please provide a valid Read token for the {CODA_DOC_ID} table"
            )
            sys.exit(1)

        super().setup()

    @property
    def items_list(self):
        return get_rows(get_columns())

    def get_item_key(self, entry) -> str:
        return html.unescape(entry["Question"])

    def _get_published_date(self, entry):
        date_published = entry["Doc Last Edited"]
        return super()._get_published_date(date_published)

    def process_entry(self, entry):
        def clean_text(text):
            text = html.unescape(text)
            return re.sub(
                r"\(/\?state=(\w+)\)", r"(http://aisafety.info?state=\1)", text
            )

        question = clean_text(
            entry["Question"]
        )  # raise an error if the entry has no question
        answer = clean_text(entry["Rich Text"])
        url = "https://aisafety.info?state=" + entry["UI ID"]

        logger.info(f"Processing {question}")

        return self.make_data_entry(
            {
                "source": self.name,
                "source_type": "markdown",
                "url": url,
                "title": question,
                "authors": ["Stampy aisafety.info"],
                "date_published": self._get_published_date(entry),
                "text": answer,
            }
        )
