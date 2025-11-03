import os
import re
import hashlib
from typing import List, Dict, Any, Optional
from pathlib import Path

from indexer.storage import get_vectorstore, init_db, persist_docling_tables
from indexer.utils_filekey import compute_file_key, normalize_path
from app.services.embeddings import get_embeddings

from docling.document_converter import DocumentConverter
from docling.chunking import HybridChunker

# --------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------

def _normalize_text(s: str) -> str:
    s = s.replace("\u00A0", " ").strip()
    return re.sub(r"\s+", " ", s)

def _hash_text(s: str) -> str:
    return hashlib.sha256(_normalize_text(s).encode("utf-8")).hexdigest()

def _normalize_path(p: str) -> str:
    try:
        return Path(p).resolve().as_posix().lower()
    except Exception:
        return str(p)


def _sanitize_for_json(obj):
    """Recursively convert an object into JSON-serializable primitives.

    - dict/list/primitive types are preserved (with recursive sanitization)
    - pydantic BaseModel -> .dict()
    - objects with .to_dict()/.dict() -> use that
    - objects with .text/.content/.value -> use that string
    - otherwise fall back to str(obj)
    """
    # primitives
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    # dicts and lists
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]

    # pydantic models
    try:
        from pydantic import BaseModel

        if isinstance(obj, BaseModel):
            return _sanitize_for_json(obj.dict())
    except Exception:
        pass

    # common conversion hooks
    for attr in ("to_dict", "dict", "as_dict"):
        if hasattr(obj, attr) and callable(getattr(obj, attr)):
            try:
                return _sanitize_for_json(getattr(obj, attr)())
            except Exception:
                pass

    for attr in ("text", "content", "value", "raw"):
        if hasattr(obj, attr):
            try:
                val = getattr(obj, attr)
                if callable(val):
                    val = val()
                return _sanitize_for_json(val)
            except Exception:
                pass

    # fall back to string representation
    try:
        return str(obj)
    except Exception:
        return None



# --------------------------------------------------------------------------------
# Docling conversion + Hybrid chunking
# --------------------------------------------------------------------------------

def _convert_with_docling(pdf_path: str):
    """
    Convert using Docling.
    """
    converter = DocumentConverter()
    return converter.convert(pdf_path).document


def _hybrid_chunks(
    docling_document: Any,
    *,
    target_tokens: int = 450,
    overlap_tokens: int = 80,
 ) -> List[Dict[str, Any]]:
    """
    Run Docling's HybridChunker to produce large, context-rich chunks.
    """

    try:

        chunker = HybridChunker(target_tokens=target_tokens, overlap_tokens=overlap_tokens)
    except Exception as e:
        # Surface a clearer message if chunker construction fails
        print(f"[ingest] HybridChunker init failed: {e}")
        raise

    return list(chunker.chunk(docling_document))


# --------------------------------------------------------------------------------
# Primary function
# --------------------------------------------------------------------------------

def upsert_document(
    meta: Dict[str, Any],
    *,
    target_tokens: int = 450,
    overlap_tokens: int = 80,
):
    """
    Convert + chunk (Hybrid) + index a single document.

    Args:
      meta: must include:
        - 'filepath' : path to the PDF to ingest
      target_tokens / overlap_tokens: sizing for HybridChunker
      tokenizer: HF tokenizer name; if None, inferred from OLLAMA_EMBED_MODEL (or defaults to 'gpt2')

    Behavior:
      - Uses Docling Hybrid chunking.
      - Serializes tables into the chunk text.
      - Inserts into PGVector.
    """
    init_db()
    vs = get_vectorstore()
    _ = get_embeddings()  # ensure embedding backend is initialized

    filepath_raw: str = meta.get("filepath")
    if not filepath_raw:
        raise ValueError("upsert_document: 'meta[\"filepath\"]' is required")

    # Compute file_key and normalize path
    file_key = compute_file_key(filepath_raw)
    source_path = normalize_path(filepath_raw)
    print(f"[ingest] Processing {source_path} → file_key={file_key}")

    # Convert with Docling and chunk with HybridChunker
    doc = _convert_with_docling(filepath_raw)
    hybrid = _hybrid_chunks(
        doc,
        target_tokens=target_tokens,
        overlap_tokens=overlap_tokens,
    )

    # Prepare payload for vector store
    texts: List[str] = []
    metadatas: List[dict] = []
    ids: List[str] = []

    for i, ch in enumerate(hybrid):
        if isinstance(ch, dict):
            text = (ch.get("text") or "").strip()
            md = dict(ch.get("meta") or {})
        else:
            raw_text = getattr(ch, "text", None) or getattr(ch, "content", None) or getattr(ch, "chunk", None)
            text = (str(raw_text) if raw_text is not None else "").strip()
            raw_meta = getattr(ch, "meta", None) or getattr(ch, "metadata", None) or {}
            try:
                md = dict(raw_meta)
            except Exception:
                md = {}

        if not text:
            continue

        # Standard metadata
        md.setdefault("type", "hybrid")
        md["file_key"] = file_key  # Use file_key in metadata
        md["source_path"] = source_path
        md["content_hash"] = _hash_text(text)

        # Use file_key in chunk ID
        chunk_id = f"{file_key}||chunk||{i}"

        texts.append(text)
        metadatas.append(md)
        ids.append(chunk_id)

    if not texts:
        # Nothing to index; return quietly
        return

    # Sanitize metadata so it can be JSON-serialized into the DB
    sanitized_metadatas = [_sanitize_for_json(m) for m in metadatas]

    # Insert new chunks with explicit ids, skipping duplicates
    try:
        vs.add_texts(texts=texts, metadatas=sanitized_metadatas, ids=ids)
        print(f"[ingest] Indexed {len(texts)} chunks")
    except Exception as e:
        if "UniqueViolation" in str(e) or "unique constraint" in str(e).lower():
            print(f"[ingest] Skipping duplicate chunks (custom_id already exists)")
        else:
            raise  # Re-raise if it's not a uniqueness violation

    # Extract and persist tables from the same document
    n_tables = persist_docling_tables(doc, file_key=file_key, source_path=source_path)
    if n_tables > 0:
        print(f"[ingest] Extracted and stored {n_tables} tables")
