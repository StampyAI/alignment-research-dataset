import io
import logging
import regex as re
from dataclasses import dataclass, field
from typing import List, Dict
from urllib.parse import urlparse, urljoin

from tqdm import tqdm

from align_data.articles.google_cloud import iterate_rows, get_spreadsheet, get_sheet, upload_file, OK, with_retry
from align_data.articles.parsers import extract_text, fetch
from align_data.settings import PDFS_FOLDER_ID


logger = logging.getLogger(__name__)


REQUIRED_FIELDS = ['url', 'source_url', 'title', 'source_type', 'date_published']
OPTIONAL_FIELDS = ['authors', 'summary']


def save_pdf(filename, link):
    """Download the pdf at `link` to the pdfs folder, saving it as `filename`.

    :param str filename: the name of the resulting file. If it doesn't end with ".pdf" that will be added
    :param str link: the url of the pdf file
    :returns: the google drive id of the resulting pdf file
    """
    res = fetch(link)
    if not filename.lower().endswith('.pdf'):
        filename += '.pdf'

    return upload_file(
        filename,
        bytes_contents=io.BytesIO(res.content),
        mimetype=res.headers.get('Content-Type'),
        parent_id=PDFS_FOLDER_ID
    )


@with_retry(times=3)
def process_row(row, sheets):
    """Check the given `row` and fetch its metadata + optional extra stuff."""
    logger.info('Checking "%s"', row['title'])

    missing = [field for field in REQUIRED_FIELDS if not row.get(field)]
    if missing:
        row.set_status('missing keys: ' + ', '.join(missing))
        logger.error('missing keys: ' + ', '.join(missing))
        return

    contents = extract_text(row['source_url'])

    if not contents or not contents.get('text'):
        error = (contents and contents.get('error')) or 'text could not be fetched'
        logger.error(error)
        row.set_status(error)
        return

    data_source = contents['data_source']
    if data_source not in sheets:
        error = 'Unhandled data type'
        logger.error(error)
        row.set_status(error)
        return

    extra_fields = []
    if data_source == 'pdf':
        extra_fields = [save_pdf(row['title'], contents['source_url'])]

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
    logger.info('fetching seen urls')
    seen = {
        url
        for sheet in output_sheets.values()
        for record in sheet.get_all_records()
        for url in [record.get('url'), record.get('source_url')]
        if url
    }
    for row in tqdm(iterate_rows(source_sheet)):
        if row.get('source_url') in seen:
            title = row.get('title')
            logger.info(f'skipping "{title}", as it has already been seen')
        else:
            process_row(row, output_sheets)


def update_new_items(source_spreadsheet, source_sheet, output_spreadsheet):
    """Go through all unprocessed items from the source worksheet, updating the appropriate metadata in the output one."""
    source_sheet = get_sheet(source_spreadsheet, source_sheet)
    sheets = {sheet.title: sheet for sheet in get_spreadsheet(output_spreadsheet).worksheets()}
    return process_spreadsheets(source_sheet, sheets)
