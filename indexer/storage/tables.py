"""
Table storage functionality for structured data extraction and persistence.
"""

import re
import json
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy import text

from .base import get_engine

def normalize_header(h: str) -> str:
    """Convert header to snake_case, removing special chars."""
    s = re.sub(r"\s+", "_", (h or "").strip())
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)
    return s.lower() or "col"

def coerce_value(v: Any) -> Any:
    """Normalize values to proper types (numbers, dates, strings)."""
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    # numeric like 1,234.56 or $1.2M / 2.5k
    m = re.match(r"^\$?(-?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)([kKmMbB])?$", s)
    if m:
        num = float(m.group(1).replace(",", "").replace(" ", ""))
        suf = m.group(2)
        if suf:
            mult = dict(k=1e3, K=1e3, m=1e6, M=1e6, b=1e9, B=1e9)[suf]
            num *= mult
        return int(num) if abs(num - int(num)) < 1e-9 else num
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):  # ISO date; cast in SQL later
        return s
    return s

def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize both keys and values in a row dict."""
    return {normalize_header(k): coerce_value(v) for k, v in row.items()}

def _cell_str(c) -> Optional[str]:
    """Extract string content from a cell with multiple fallbacks."""
    for attr in ("text", "content", "value", "plain_text", "ocr_text"):
        v = getattr(c, attr, None)
        if isinstance(v, str) and v.strip(): return v.strip()
    if isinstance(c, str): return c.strip()
    try:
        s = str(c).strip()
        return s or None
    except Exception:
        return None

def table_to_structured(tbl) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Extract headers and rows from a table with multiple methods.
    
    Returns:
        Tuple of (headers, rows) where headers is a list of column names
        and rows is a list of dicts mapping column names to values.
    """
    # Official exporters
    to_df = getattr(tbl, "export_to_dataframe", None) or getattr(tbl, "to_pandas", None)
    if callable(to_df):
        try:
            df = to_df(doc=True)
            if df is not None and getattr(df, "empty", False) is False:
                headers = [str(h) for h in list(df.columns)]
                rows = [{str(h): (None if v is None else str(v)) for h, v in rec.items()}
                        for rec in df.to_dict(orient="records")]
                return headers, rows
        except Exception as e:
            print(f"[DEBUG] Table export failed: {e}")
            pass
    # columns/rows API
    cols = getattr(tbl, "columns", None); trs = getattr(tbl, "rows", None)
    if cols is not None and trs is not None:
        headers = [_cell_str(c) or "" for c in cols]
        out = []
        for r in trs:
            cells = getattr(r, "cells", None) or []
            row = {}
            for i, h in enumerate(headers):
                row[h or f"col_{i}"] = _cell_str(cells[i]) if i < len(cells) else None
            out.append(row)
        if not any(headers):
            n = len(out[0]) if out else 0
            headers = [f"col_{i}" for i in range(n)]
            out = [{f"col_{i}": v for i, v in enumerate(r.values())} for r in out]
        return headers, out
    # CSV fallback
    for meth in ("export_to_csv", "to_csv"):
        f = getattr(tbl, meth, None)
        if callable(f):
            import csv, io
            try:
                csv_text = f()
                if csv_text:
                    rows_list = list(csv.reader(io.StringIO(csv_text)))
                    headers = [h.strip() for h in rows_list[0]] if rows_list else []
                    out = []
                    for rr in rows_list[1:]:
                        row = {}
                        for i, h in enumerate(headers):
                            row[h] = rr[i].strip() if i < len(rr) else None
                        out.append(row)
                    return headers, out
            except Exception:
                pass
    return [], []

def persist_docling_tables(doc, *, file_key: str, source_path: str, vectorstore=None) -> int:
    """Extract all tables from Docling doc and upsert into Postgres JSONB.
    Also creates schema-level embeddings in vector store for analytical queries.
    
    Args:
        doc: Docling document object with tables attribute
        file_key: Stable identifier from file content hash
        source_path: Path to source file
        vectorstore: Optional vectorstore instance for schema embeddings
        
    Returns:
        Number of tables successfully written
    """
    tables = getattr(doc, "tables", None) or []
    if not tables:
        return 0
        
    # Lists to collect schema embeddings if vectorstore provided
    schema_texts = []
    schema_metadatas = []
    schema_ids = []
        
    engine = get_engine()
    written = 0
    
    with engine.begin() as conn:
        for t_idx, tbl in enumerate(tables):
            headers, rows = table_to_structured(tbl)
            if not headers and not rows:
                continue
                
            norm_headers = [normalize_header(h) for h in headers]
            norm_rows = [normalize_row(r) for r in rows]
            
            # Generate schema description for embedding
            if vectorstore:
                # Create a natural language description of the table schema
                schema_text = f"file={source_path}; table_index={t_idx}; columns: {', '.join(headers)}; rows={len(norm_rows)}"
                schema_metadata = {
                    "type": "table_schema",
                    "file_key": file_key,
                    "table_index": t_idx,
                    "headers": headers,
                    "n_rows": len(norm_rows)
                }
                schema_id = f"{file_key}||table||{t_idx}||schema"
                
                schema_texts.append(schema_text)
                schema_metadatas.append(schema_metadata)
                schema_ids.append(schema_id)
            
            # catalog
            conn.execute(text("""
              INSERT INTO tables_catalog(file_key, table_index, column_names, n_rows, source_path)
              VALUES (:k,:i,:c,:n,:s)
              ON CONFLICT (file_key, table_index) DO UPDATE
                SET column_names = EXCLUDED.column_names,
                    n_rows       = EXCLUDED.n_rows,
                    source_path  = EXCLUDED.source_path
            """), {"k": file_key, "i": t_idx, "c": norm_headers, "n": len(norm_rows), "s": source_path})
            
            # rows
            if norm_rows:
                params = [{"k": file_key, "i": t_idx, "r": r_idx, "j": json.dumps(norm_rows[r_idx])}
                          for r_idx in range(len(norm_rows))]
                conn.execute(text("""
                  INSERT INTO table_rows(file_key, table_index, row_index, data)
                  VALUES (:k,:i,:r,:j)
                  ON CONFLICT (file_key, table_index, row_index) DO UPDATE
                    SET data = EXCLUDED.data
                """), params)
            written += 1
            
    # Store schema embeddings if vectorstore provided
    if vectorstore and schema_texts:
        try:
            vectorstore.add_texts(
                texts=schema_texts,
                metadatas=schema_metadatas,
                ids=schema_ids
            )
            print(f"[tables] Created {len(schema_texts)} table schema embeddings")
        except Exception as e:
            if "UniqueViolation" in str(e) or "unique constraint" in str(e).lower():
                print("[tables] Skipping duplicate schema embeddings")
            else:
                raise
            
    return written