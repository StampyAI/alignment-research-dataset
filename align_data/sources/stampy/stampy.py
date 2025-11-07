import sys
import re
import logging
import requests
from dataclasses import dataclass

import html

from align_data.common.alignment_dataset import AlignmentDataset
from align_data.settings import CODA_TOKEN, CODA_DOC_ID, ON_SITE_TABLE

logger = logging.getLogger(__name__)


headers = {"Authorization": f"Bearer {CODA_TOKEN}"}


def get_columns():
    uri = f"https://coda.io/apis/v1/docs/{CODA_DOC_ID}/tables/{ON_SITE_TABLE}/columns"
    logger.info(f"Fetching columns from {uri}")

    try:
        response = requests.get(uri, headers=headers, timeout=30)
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to {uri} failed: {e}")
        raise

    logger.info(f"Response status code: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Request to {uri} failed with status {response.status_code}")
        logger.error(f"Response: {response.text[:1000]}")
        raise Exception(f"Failed to get columns: {response.status_code} - {response.text[:200]}")

    try:
        data = response.json()
    except Exception as e:
        logger.error(f"Failed to parse JSON response from {uri}")
        logger.error(f"Response: {response.text[:1000]}")
        raise Exception(f"Failed to parse JSON from {uri}: {e}")

    if "items" not in data:
        logger.error(f"Response missing 'items' field. Response: {data}")
        raise Exception(f"Response missing 'items' field")

    return {c["id"]: c["name"] for c in data["items"]}


def paginated(url):
    logger.info(f"Fetching page from {url}")

    try:
        response = requests.get(url, headers=headers, timeout=30)
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to {url} failed: {e}")
        raise

    logger.info(f"Response status code: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Request to {url} failed with status {response.status_code}")
        logger.error(f"Response: {response.text[:1000]}")
        raise Exception(f"Failed to fetch page: {response.status_code} - {response.text[:200]}")

    try:
        resp = response.json()
    except Exception as e:
        logger.error(f"Failed to parse JSON response from {url}")
        logger.error(f"Response: {response.text[:1000]}")
        raise Exception(f"Failed to parse JSON from {url}: {e}")

    if "items" not in resp:
        logger.error(f"Response missing 'items' field. Response: {resp}")
        raise Exception(f"Response missing 'items' field")

    for row in resp["items"]:
        yield row
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
