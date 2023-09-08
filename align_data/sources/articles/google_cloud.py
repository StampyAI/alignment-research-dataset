import logging
import time
from collections import UserDict
from pathlib import Path
from typing import Dict, Any, Iterator, Union, List, Set
import re

import requests
import gdown
import grobid_tei_xml
import gspread
from gspread.worksheet import Worksheet
from gspread.spreadsheet import Spreadsheet
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from markdownify import MarkdownConverter

from align_data.sources.articles.html import fetch, fetch_element
from align_data.sources.articles.pdf import fetch_pdf

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

OK = "ok"
OUTPUT_SPREADSHEET_ID = "1bg-6vL-I82CBRkxvWQs1-Ao0nTvHyfn4yns5MdlbCmY" # TODO: remove this
sheet_name = "Sheet1" # TODO: remove this


def get_credentials(credentials_file: Union[Path, str] = "credentials.json") -> Credentials:
    return Credentials.from_service_account_file(credentials_file, scopes=SCOPES)


def get_spreadsheet(spreadsheet_id: str, credentials: Credentials = None) -> Spreadsheet:
    client = gspread.authorize(credentials or get_credentials())
    return client.open_by_key(spreadsheet_id)


def get_sheet(spreadsheet_id: str, sheet_name: str, credentials: Credentials = None) -> Worksheet:
    spreadsheet = get_spreadsheet(spreadsheet_id, credentials)
    return spreadsheet.worksheet(title=sheet_name)


class SheetRow(UserDict):
    """A row in a Google Sheet."""
    sheet = None # type: Worksheet
    columns = None # type: List[str | None]

    @classmethod
    def set_sheet(cls, sheet: Worksheet):
        cls.sheet = sheet
        cols = sheet.row_values(1)
        # if there is no first column, we raise an error
        if not isinstance(cols, list) or not cols:
            raise ValueError(f"Sheet {sheet.title} has no header row")

        cls.columns = cols

    def __init__(self, row_id: int, data: Dict[str, Any]):
        self.row_id = row_id
        super().__init__(data)

    def update_value(self, col: str, value: str):
        self.sheet.update_cell(self.row_id, self.columns.index(col) + 1, value)

    def update_colour(self, col: str, colour: Dict[str, float]):
        col_letter = chr(ord("A") + self.columns.index(col))
        self.sheet.format(f"{col_letter}{self.row_id}", {"backgroundColor": colour})

    def set_status(self, status: str, status_col: str = "status"):
        if self.get(status_col) == status:
            # Don't update anything if the status is the same - this saves on gdocs calls
            return

        if status == OK:
            colour = {"red": 0.0, "green": 1.0, "blue": 0.0}
        elif status == "": # TODO: this should never be reached
            colour = {"red": 1.0, "green": 1.0, "blue": 1.0}
        else:
            colour = {"red": 1.0, "green": 0.0, "blue": 0.0}

        self.update_value(status_col, status)
        self.update_colour(status_col, colour)


def iterate_rows(sheet: Worksheet) -> Iterator[SheetRow]:
    """Iterate over all the rows of the given `sheet`."""
    SheetRow.set_sheet(sheet)

    # we start the enumeration at 2 to avoid the header row
    for i, row_data in enumerate(sheet.get_all_records(), 2):
        yield SheetRow(i, row_data)


def upload_file(filename, bytes_contents, mimetype, parent_id=None):
    """Create a google drive file called `filename` containing `bytes_contents`.

    :param str filename: The name of the resulting file
    :param bytes bytes_contents: The raw contents of the resulting file
    :param str mimetype: The mimetype of the file
    :param str parent_id: The id of the folder of the file
    :returns: The google drive id of the resulting file
    """
    credentials = get_credentials()

    drive_service = build("drive", "v3", credentials=credentials)

    file_metadata = {"name": filename, "parents": parent_id and [parent_id]}
    media = (
        drive_service.files()
        .create(
            body=file_metadata,
            media_body=MediaIoBaseUpload(bytes_contents, mimetype=mimetype),
        )
        .execute()
    )
    return media.get("id")


def with_retry(times=3):
    """A decorator that will retry the wrapped function up to `times` times in case of google sheets errors."""

    def wrapper(f):
        def retrier(*args, **kwargs):
            for i in range(times):
                try:
                    return f(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    logger.error(f"{e} - retrying up to {times - i} times")
                    # Do a logarithmic backoff
                    time.sleep((i + 1) ** 2)
            raise ValueError(f"Gave up after {times} tries")

        return retrier

    return wrapper


def fetch_file(file_id: str):
    data_path = Path("data/raw/")
    data_path.mkdir(parents=True, exist_ok=True)
    file_name = data_path / file_id
    return gdown.download(id=file_id, output=str(file_name), quiet=False)


def fetch_markdown(file_id: str) -> Dict[str, str]:
    try:
        file_name = fetch_file(file_id)
        return {
            "text": Path(file_name).read_text(),
            "source_type": "markdown",
        }
    except Exception as e:
        return {"error": str(e)}


def parse_grobid(contents: str | bytes) -> Dict[str, Any]:
    if isinstance(contents, bytes):
        contents = contents.decode('utf-8')
    doc_dict = grobid_tei_xml.parse_document_xml(contents).to_dict()
    authors: List[str] = [author["full_name"].strip(" !") for author in doc_dict.get("header", {}).get("authors", [])]

    if not doc_dict.get("body"):
        return {
            "error": "No contents in XML file",
            "source_type": "xml",
        }

    return {
        "title": doc_dict.get("header", {}).get("title"),
        "abstract": doc_dict.get("abstract"),
        "text": doc_dict["body"],
        "authors": list(filter(None, authors)),
        "source_type": "xml",
    }


def get_content_type(res: requests.Response) -> Set[str]:
    header = res.headers.get("Content-Type") or ""
    parts = [c_type.strip().lower() for c_type in header.split(";")]
    return set(filter(None, parts))


def extract_gdrive_contents(link: str) -> Dict[str, Any]:
    file_id = link.split("/")[-2]
    url = f"https://drive.google.com/uc?id={file_id}"
    res = fetch(url, "head")
    if res.status_code == 403:
        logger.error("Could not fetch the file at %s - 403 returned", link)
        return {"error": "Could not read file from google drive - forbidden"}
    if res.status_code >= 400:
        logger.error("Could not fetch the file at %s - are you sure that link is correct?", link)
        return {"error": "Could not read file from google drive"}

    result: Dict[str, Any] = {
        'source_url': link,
        'downloaded_from': 'google drive',
    }

    content_type = get_content_type(res)
    if not content_type:
        result["error"] = "no content type"
    elif content_type & {"application/octet-stream", "application/pdf"}:
        result.update(fetch_pdf(url))
    elif content_type & {"text/markdown"}:
        result.update(fetch_markdown(file_id))
    elif content_type & {"application/epub+zip", "application/epub"}:
        result["source_type"] = "ebook"
    elif content_type & {"text/html"}:
        res = fetch(url)
        if "Google Drive - Virus scan warning" in res.text:
            soup = BeautifulSoup(res.content, "html.parser")
            
            form_tag = soup.select_one('form')
            if form_tag is None:
                return {**result, 'error': 'Virus scan warning - no form tag'}
            
            form_action_url = form_tag.get('action')
            if not isinstance(form_action_url, str):
                return {**result, 'error': 'Virus scan warning - no form action url'}
            
            res = fetch(form_action_url)

        content_type = get_content_type(res)
        if content_type & {"text/xml"}:
            result.update(parse_grobid(res.content))
        elif content_type & {"text/html"}:
            soup = BeautifulSoup(res.content, "html.parser")
            result.update(
                {
                    "text": MarkdownConverter().convert_soup(soup.select_one("body")).strip(),
                    "source_type": "html",
                }
            )
        else:
            result["error"] = f"unknown content type: {content_type}"
    else:
        result["error"] = f"unknown content type: {content_type}"

    return result


def google_doc(url: str) -> Dict[str, Any]:
    """Fetch the contents of the given gdoc url as markdown."""
    res = re.search(r"https://docs.google.com/document/(?:u/)?(?:0/)?d/(.*?)/", url)
    if not res:
        return {}

    doc_id = res.group(1)
    body = fetch_element(f"https://docs.google.com/document/d/{doc_id}/export?format=html", "body")
    if body:
        return {
            "text": MarkdownConverter().convert_soup(body).strip(),
            "source_url": url,
        }
    return {}
