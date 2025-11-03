"""
Base database functionality shared between vector and table storage.
"""

import os
from sqlalchemy import create_engine, text
from typing import Optional

_engine = None

def get_engine():
    """Get SQLAlchemy engine, creating it if needed."""
    global _engine
    if _engine is None:
        url = os.getenv("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL not set")
        _engine = create_engine(url, future=True)
    return _engine

def init_db():
    """Initialize database schema for table storage
    (Vector storage schema is handled by langchain-pgvector)."""
    engine = get_engine()
    with engine.begin() as conn:
        # Ensure pgvector extension exists
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        
        # Table storage schema
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS tables_catalog (
            file_key     TEXT NOT NULL,
            table_index  INTEGER NOT NULL,
            column_names TEXT[] NOT NULL,
            n_rows       INTEGER NOT NULL,
            source_path  TEXT,
            PRIMARY KEY (file_key, table_index)
        );"""))
        
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS table_rows (
            file_key     TEXT NOT NULL,
            table_index  INTEGER NOT NULL,
            row_index    INTEGER NOT NULL,
            data         JSONB NOT NULL,
            PRIMARY KEY (file_key, table_index, row_index)
        );"""))
        
        # Indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_table_rows_file_tbl ON table_rows(file_key, table_index);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_table_rows_gin ON table_rows USING GIN (data jsonb_path_ops);"))