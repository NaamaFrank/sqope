"""
Vector storage functionality for document embeddings.
"""

from langchain_community.vectorstores.pgvector import PGVector
from app.services.embeddings import get_embeddings

from .base import get_engine

_vectorstore = None
COLLECTION_NAME = "sqope_docs" 

def get_vectorstore():
    """Get or create PGVector instance for document storage."""
    global _vectorstore
    if _vectorstore is None:
        embeddings = get_embeddings()
        import os
        _vectorstore = PGVector(
            embedding_function=embeddings,
            connection_string=os.getenv("DATABASE_URL"),
            collection_name=COLLECTION_NAME,
            use_jsonb=True,
            pre_delete_collection=False,
        )
        
    return _vectorstore