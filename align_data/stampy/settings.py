import os
import sys
from dotenv import load_dotenv
load_dotenv()
CODA_DOC_ID = os.environ.get("CODA_DOC_ID", "fau7sl2hmG")
ON_SITE_TABLE = os.environ.get('CODA_ON_SITE_TABLE', 'table-aOTSHIz_mN')

CODA_TOKEN = os.environ.get("CODA_TOKEN")
if not CODA_TOKEN:
    print(f'No CODA_TOKEN found! Please provide a valid Read token for the {CODA_DOC_ID} table')
    sys.exit(1)
