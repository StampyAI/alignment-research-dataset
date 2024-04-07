import os
import logging
from typing import Dict
import openai
import torch
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

### MYSQL ###
DB_CONNECTION_URI = os.environ.get("ARD_DB_CONNECTION_URI", "mysql+mysqlconnector://user:we all live in a yellow submarine@127.0.0.1:3306/alignment_research_dataset")
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
USE_OPENAI_EMBEDDINGS = True  # If false, SentenceTransformer embeddings will be used.
EMBEDDING_LENGTH_BIAS: Dict[str, float] = {
    # TODO: Experiement with these values. For now, let's remove the bias.
    # "aisafety.info": 1.05,  # In search, favor AISafety.info entries.
}

OPENAI_EMBEDDINGS_MODEL = "text-embedding-ada-002"
OPENAI_EMBEDDINGS_DIMS = 1536
OPENAI_EMBEDDINGS_RATE_LIMIT = 3500
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", None)
OPENAI_ORGANIZATION = os.environ.get("OPENAI_ORGANIZATION", None)

SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL = "sentence-transformers/multi-qa-mpnet-base-cos-v1"
SENTENCE_TRANSFORMER_EMBEDDINGS_DIMS = 768

### PINECONE ###
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "stampy-chat-ard")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", None)
PINECONE_ENVIRONMENT = os.environ.get("PINECONE_ENVIRONMENT", None)
PINECONE_VALUES_DIMS = (
    OPENAI_EMBEDDINGS_DIMS if USE_OPENAI_EMBEDDINGS else SENTENCE_TRANSFORMER_EMBEDDINGS_DIMS
)
PINECONE_METRIC = "dotproduct"
PINECONE_NAMESPACE = os.environ.get("PINECONE_NAMESPACE", "normal")  # "normal" or "finetuned"

### FINE-TUNING ###
OPENAI_FINETUNED_LAYER_PATH = os.environ.get(
    "OPENAI_FINETUNED_LAYER_PATH", "align_data/finetuning/data/finetuned_model.pth"
)
OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH = os.environ.get(
    "OPENAI_CURRENT_BEST_FINETUNED_LAYER_PATH",
    "align_data/finetuning/data/best_finetuned_model.pth",
)

### MISCELLANEOUS ###
MIN_CONFIDENCE = float(os.environ.get('MIN_CONFIDENCE') or '0.5')
if MIN_CONFIDENCE < 0 or MIN_CONFIDENCE > 1:
    raise ValueError(f'MIN_CONFIDENCE must be between 0 and 1 - got {MIN_CONFIDENCE}')

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
