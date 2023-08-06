import os
from dotenv import load_dotenv
load_dotenv()

### CODA ###
CODA_TOKEN = os.environ.get("CODA_TOKEN")
CODA_DOC_ID = os.environ.get("CODA_DOC_ID", "fau7sl2hmG")
ON_SITE_TABLE = os.environ.get('CODA_ON_SITE_TABLE', 'table-aOTSHIz_mN')

### GOOGLE DRIVE ###
PDFS_FOLDER_ID = os.environ.get('PDFS_FOLDER_ID', '1etWiXPRl0QqdgYzivVIj6wCv9xj5VYoN')

### GOOGLE SHEETS ###
METADATA_SOURCE_SPREADSHEET = os.environ.get('METADATA_SOURCE_SPREADSHEET', '1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI')
METADATA_SOURCE_SHEET = os.environ.get('METADATA_SOURCE_SHEET', 'special_docs.csv')
METADATA_OUTPUT_SPREADSHEET = os.environ.get('METADATA_OUTPUT_SPREADSHEET', '1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4')

### YouTube ###
YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY')

### MYSQL ###
user = os.environ.get('ARD_DB_USER', 'user')
password = os.environ.get('ARD_DB_PASSWORD', 'we all live in a yellow submarine')
host = os.environ.get('ARD_DB_HOST', '127.0.0.1')
port = os.environ.get('ARD_DB_PORT', '3306')
db_name = os.environ.get('ARD_DB_NAME', 'alignment_research_dataset')
DB_CONNECTION_URI = f'mysql+mysqldb://{user}:{password}@{host}:{port}/{db_name}'

### EMBEDDINGS ###
USE_OPENAI_EMBEDDINGS = True  # If false, SentenceTransformer embeddings will be used.
EMBEDDING_LENGTH_BIAS = {
    "aisafety.info": 1.05,  # In search, favor AISafety.info entries.
}

OPENAI_EMBEDDINGS_MODEL = "text-embedding-ada-002"
OPENAI_EMBEDDINGS_DIMS = 1536
OPENAI_EMBEDDINGS_RATE_LIMIT = 3500

SENTENCE_TRANSFORMER_EMBEDDINGS_MODEL = "sentence-transformers/multi-qa-mpnet-base-cos-v1"
SENTENCE_TRANSFORMER_EMBEDDINGS_DIMS = 768

### PINECONE ###
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "stampy-chat-ard")
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", None)
PINECONE_ENVIRONMENT = os.environ.get("PINECONE_ENVIRONMENT", None)
PINECONE_VALUES_DIMS = OPENAI_EMBEDDINGS_DIMS if USE_OPENAI_EMBEDDINGS else SENTENCE_TRANSFORMER_EMBEDDINGS_DIMS
PINECONE_METRIC = "dotproduct"
PINECONE_METADATA_ENTRIES = ["entry_id", "source", "title", "authors", "text"]

### MISCELLANEOUS ###
CHUNK_SIZE = 1750
MAX_NUM_AUTHORS_IN_SIGNATURE = 3
