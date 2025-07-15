import os
import logging
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "WARNING").upper()
logging.basicConfig(level=LOG_LEVEL)

### CODA ###
CODA_TOKEN = os.environ.get("CODA_TOKEN")
CODA_DOC_ID = os.environ.get("CODA_DOC_ID", "fau7sl2hmG")
ON_SITE_TABLE = os.environ.get("CODA_ON_SITE_TABLE", "table-aOTSHIz_mN")

### GOOGLE DRIVE ###
PDFS_FOLDER_ID = os.environ.get("PDFS_FOLDER_ID", "1etWiXPRl0QqdgYzivVIj6wCv9xj5VYoN")

### GOOGLE SHEETS ###
METADATA_SOURCE_SPREADSHEET = os.environ.get(
    "METADATA_SOURCE_SPREADSHEET", "1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI"
)
METADATA_SOURCE_SHEET = os.environ.get("METADATA_SOURCE_SHEET", "special_docs.csv")
METADATA_OUTPUT_SPREADSHEET = os.environ.get(
    "METADATA_OUTPUT_SPREADSHEET", "1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4"
)

### YOUTUBE ###
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

### Airtable ###
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AGISF_AIRTABLE_BASE_ID = os.environ.get("AGISF_AIRTABLE_BASE_ID")
AGISF_AIRTABLE_TABLE_ID = os.environ.get("AGISF_AIRTABLE_TABLE_ID")

### MYSQL ###
if not (DB_CONNECTION_URI := os.environ.get("ARD_DB_CONNECTION_URI")):
    user = os.environ.get("ARD_DB_USER", "user")
    password = os.environ.get("ARD_DB_PASSWORD", "we all live in a yellow submarine")
    host = os.environ.get("ARD_DB_HOST", "127.0.0.1")
    port = os.environ.get("ARD_DB_PORT", "3306")
    db_name = os.environ.get("ARD_DB_NAME", "alignment_research_dataset")
    DB_CONNECTION_URI = (
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}"
    )

ARTICLE_MAIN_KEYS = [
    "id",
    "source",
    "source_type",
    "title",
    "authors",
    "text",
    "url",
    "date_published",
    "status",
    "comments",
]

### EMBEDDINGS ###
DEFAULT_CHUNK_TOKENS = 512
OVERLAP_TOKENS = 50

VOYAGEAI_API_KEY = os.environ.get("VOYAGEAI_API_KEY")
VOYAGEAI_EMBEDDINGS_MODEL = os.environ.get(
    "VOYAGEAI_EMBEDDINGS_MODEL", "voyage-3-large"
)
EMBEDDINGS_DIMS = 1024
USE_MODERATION = os.environ.get("USE_MODERATION", "true").lower() == "true"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_ORGANIZATION = os.environ.get("OPENAI_ORGANIZATION")

### PINECONE ###
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "stampy-chat-ard")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", None)
PINECONE_ENVIRONMENT = os.environ.get("PINECONE_ENVIRONMENT", None)
PINECONE_METRIC = "dotproduct"
PINECONE_NAMESPACE = os.environ.get(
    "PINECONE_NAMESPACE", "normal"
)  # "normal" or "finetuned"

### MISCELLANEOUS ###
MIN_CONFIDENCE = float(os.environ.get("MIN_CONFIDENCE") or "0.5")
if MIN_CONFIDENCE < 0 or MIN_CONFIDENCE > 1:
    raise ValueError(f"MIN_CONFIDENCE must be between 0 and 1 - got {MIN_CONFIDENCE}")
