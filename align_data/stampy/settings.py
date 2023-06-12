import os
from dotenv import load_dotenv
load_dotenv()
CODA_TOKEN = os.environ.get("CODA_TOKEN", "token not found")
CODA_DOC_ID = os.environ.get("CODA_DOC_ID", "fau7sl2hmG")
ON_SITE_TABLE = os.environ.get('CODA_ON_SITE_TABLE', 'table-aOTSHIz_mN')