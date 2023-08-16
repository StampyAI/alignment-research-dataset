import io
import logging

from tqdm import tqdm
import gspread

from align_data.sources.articles.google_cloud import (
    iterate_rows,
    get_spreadsheet,
    get_sheet,
    upload_file,
    OK,
    with_retry,
)
from align_data.sources.articles.parsers import item_metadata, fetch
from align_data.sources.articles.indices import fetch_all
from align_data.sources.articles.html import with_retry
from align_data.sources.articles.updater import ReplacerDataset
from align_data.settings import PDFS_FOLDER_ID


logger = logging.getLogger(__name__)


# Careful changing these - the sheets assume this ordering
REQUIRED_FIELDS = ["url", "source_url", "title", "source_type", "date_published"]
OPTIONAL_FIELDS = ["authors", "summary"]


def save_pdf(filename, link):
    """Download the pdf at `link` to the pdfs folder, saving it as `filename`.

    :param str filename: the name of the resulting file. If it doesn't end with ".pdf" that will be added
    :param str link: the url of the pdf file
    :returns: the google drive id of the resulting pdf file
    """
    res = fetch(link)
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"

    return upload_file(
        filename,
        bytes_contents=io.BytesIO(res.content),
        mimetype=res.headers.get("Content-Type"),
        parent_id=PDFS_FOLDER_ID,
    )

# Sometimes the api takes 2-3 minutes before it starts working again
# During those times you can set times=10
@with_retry(times=3, exceptions=gspread.exceptions.APIError)
def process_row(row, sheets):
    """Check the given `row` and fetch its metadata + optional extra stuff."""
    logger.info('Checking "%s" at "%s', row["title"], row["url"])

    missing = [field for field in REQUIRED_FIELDS if not row.get(field)]
    if missing:
        row.set_status("missing keys: " + ", ".join(missing))
        logger.error("missing keys: " + ", ".join(missing))
        return

    source_url = row.get("source_url")
    contents = item_metadata(source_url)

    if not contents or "error" in contents:
        error = (contents and contents.get("error")) or "text could not be fetched"
        logger.error(error)
        row.set_status(error)
        return

    data_source = contents.get("source_type")
    if data_source not in sheets:
        error = "Unhandled data type"
        logger.error(error)
        row.set_status(error)
        return

    extra_fields = []
    if data_source == "pdf":
        extra_fields = [save_pdf(row["title"], source_url)]

    sheets[data_source].append_row(
        [row.get(field) for field in REQUIRED_FIELDS + OPTIONAL_FIELDS] + extra_fields
    )
    row.set_status(OK)


def process_spreadsheets(source_sheet, output_sheets):
    """Go through all entries in `source_sheet` and update the appropriate metadata in `output_sheets`.

    `output_sheets` should be a dict with a key for each possible data type, e.g. html, pdf etc.

    :param Worksheet source_sheet: the worksheet to be processed - each row should be a separate entry
    :param Dict[str, Worksheet] output_sheets: a dict of per data type worksheets to be updated
    """
    logger.info("fetching seen urls")
    seen = {
        url
        for sheet in output_sheets.values()
        for record in sheet.get_all_records()
        for url in [record.get("url"), record.get("source_url")]
        if url
    } # Note: This requires our output_sheet to already have the headers for the different sheets

    for row in tqdm(iterate_rows(source_sheet)):
        title = row.get("title")
        if not row.get("source_url"):
            row["source_url"] = row["url"]
        if row.get("source_url") in seen:
            logger.info(f'skipping "{title}", as it has already been seen')
        # elif row.get('status'):
        #     logger.info(f'skipping "{title}", as it has a status set - remove it for this row to be processed')
        else:
            process_row(row, output_sheets)


def update_new_items(source_spreadsheet, source_sheet, output_spreadsheet):
    """Go through all unprocessed items from the source worksheet, updating the appropriate metadata in the output one."""
    source_sheet = get_sheet(source_spreadsheet, source_sheet)
    output_sheets = {
        sheet.title: sheet for sheet in get_spreadsheet(output_spreadsheet).worksheets()
    }
    return process_spreadsheets(source_sheet, output_sheets)


def check_new_articles(source_spreadsheet, source_sheet):
    """Goes through the special indices looking for unseen articles."""
    source_sheet = get_sheet(source_spreadsheet, source_sheet)
    current = {row.get("title"): row for row in iterate_rows(source_sheet)}
    logger.info('Found %s articles in the sheet', len(current))

    # get all the urls from the sheet. they are in the 'url' and 'source_url' fields
    seen_urls = {
        url
        for item in current.values()
        for key in ("url", "source_url")
        if (url := item.get(key)) is not None
    }

    indices_items = fetch_all()

    missing = [
        item
        for title, item in indices_items.items()
        if title not in current
        and not {item.get("url"), item.get("source_url")} & seen_urls
    ]
    if not missing:
        logger.info("No new articles found")
        return 0

    columns = [
        "status",
        "source_url",
        "url",
        "title",
        "date_published",
        "authors",
        "publication_title",
        "source_type",
    ]
    res = source_sheet.append_rows(
        [[item.get(col) for col in columns] for item in missing]
    )
    updated = res["updates"]["updatedRows"]
    logger.info("Added %s rows", updated)
    return updated


def update_articles(csv_file, delimiter):
    dataset = ReplacerDataset(name='updater', csv_path=csv_file, delimiter=delimiter)
    dataset.add_entries(dataset.fetch_entries())
