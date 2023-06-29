import os
from dotenv import load_dotenv
load_dotenv()


CODA_TOKEN = os.environ.get("CODA_TOKEN")
CODA_DOC_ID = os.environ.get("CODA_DOC_ID", "fau7sl2hmG")
ON_SITE_TABLE = os.environ.get('CODA_ON_SITE_TABLE', 'table-aOTSHIz_mN')

PDFS_FOLDER_ID = os.environ.get('PDF_FOLDER_ID', '1etWiXPRl0QqdgYzivVIj6wCv9xj5VYoN')
