# dataset/sql_db_handler.py

from typing import List, Dict, Union
import numpy as np
import sqlite3

from align_data.settings import SQL_DB_PATH

import logging
logger = logging.getLogger(__name__)


class SQLDB:
    def __init__(self, db_name: str = SQL_DB_PATH):
        self.db_name = db_name
        
        self.create_tables()

    def create_tables(self, reset: bool = False):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                if reset:
                    # Drop the tables if reset is True
                    cursor.execute("DROP TABLE IF EXISTS entry_database")
                    cursor.execute("DROP TABLE IF EXISTS chunk_database")
                
                # Create entry table
                query = """
                    CREATE TABLE IF NOT EXISTS entry_database (
                        id TEXT PRIMARY KEY,
                        source TEXT,
                        title TEXT,
                        text TEXT,
                        url TEXT,
                        date_published TEXT,
                        authors TEXT
                    )
                """
                cursor.execute(query)

                # Create chunk table
                query = """
                    CREATE TABLE IF NOT EXISTS chunk_database (
                        id TEXT PRIMARY KEY,
                        text TEXT,
                        embedding BLOB,
                        entry_id TEXT,
                        FOREIGN KEY (entry_id) REFERENCES entry_database(id)
                    )
                """
                cursor.execute(query)

            except sqlite3.Error as e:
                logger.error(f"The error '{e}' occurred.")

    def upsert_entry(self, entry: Dict[str, Union[str, list]]) -> bool:
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                # Fetch existing data
                cursor.execute("SELECT * FROM entry_database WHERE id=?", (entry['id'],))
                existing_entry = cursor.fetchone()

                new_entry = (
                    entry['id'],
                    entry['source'],
                    entry['title'],
                    entry['text'],
                    entry['url'],
                    entry['date_published'],
                    ', '.join(entry['authors'])
                )

                if existing_entry != new_entry:
                    query = """
                        INSERT OR REPLACE INTO entry_database
                        (id, source, title, text, url, date_published, authors)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    cursor.execute(query, new_entry)
                    return True
                else:
                    return False

            except sqlite3.Error as e:
                logger.error(f"The error '{e}' occurred.")
                return False

            finally:
                conn.commit()
                
    def upsert_chunks(self, chunks_ids_batch: List[str], chunks_batch: List[str], embeddings_batch: List[np.ndarray]) -> bool:
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                for chunk_id, chunk, embedding in zip(chunks_ids_batch, chunks_batch, embeddings_batch):
                    cursor.execute("""
                        INSERT OR REPLACE INTO chunk_database
                        (id, text, embedding)
                        VALUES (?, ?, ?)
                    """, (chunk_id, chunk, embedding.tobytes()))
            except sqlite3.Error as e:
                logger.error(f"The error '{e}' occurred.")
            finally:
                conn.commit()

                
    def stream_chunks(self):
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()

            # Join entry_database and chunk_database tables and order by source
            cursor.execute("""
                SELECT c.id, c.text, c.embedding, e.source 
                FROM chunk_database c
                JOIN entry_database e ON c.entry_id = e.id
                ORDER BY e.source
            """)

            for row in cursor:
                # Convert bytes back to numpy array
                embedding = np.frombuffer(row[2], dtype=np.float64) if row[2] else None

                yield {
                    'id': row[0],
                    'text': row[1],
                    'embedding': embedding,
                    'source': row[3],
                }