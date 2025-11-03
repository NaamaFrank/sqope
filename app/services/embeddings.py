import os
from langchain_ollama import OllamaEmbeddings

def get_embeddings():
    return OllamaEmbeddings(
        base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        model=os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
    )
