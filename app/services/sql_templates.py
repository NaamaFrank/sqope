"""Centralized SQL templates and helpers for the project.
"""
from typing import List, Dict, Any, Optional

# Simple named templates (use with SQLAlchemy text(...))
SELECT_COLUMN_NAMES = """SELECT column_names
FROM tables_catalog
WHERE file_key=:k AND table_index=:i"""

SELECT_TABLE_ROWS_SAMPLE = """SELECT data
FROM table_rows
WHERE file_key=:k AND table_index=:i
ORDER BY row_index
LIMIT :n"""

def build_select_query(sel: List[str], where_parts: List[str], group_by_exprs: Optional[List[str]] = None,
                       order_by: Optional[List[Dict[str, Any]]] = None, limit: int = 0) -> str:
    """Assemble a SELECT query for the logical `table_rows` storage.

    Inputs must be validated prior to calling this helper:
      - `sel`: list of already-quoted/select-expression strings (e.g. "(data->>'col') AS \"col\"")
      - `where_parts`: list of strings (already-safe fragments like "file_key = :fk")
      - `group_by_exprs`: list of expressions to use in GROUP BY
      - `order_by`: optional list with one dict {"column": <name>, "dir": "asc"|"desc"}
      - `limit`: integer limit (0 means no limit)

    Returns a SQL string. This helper intentionally does not bind parameters.
    """
    sql = f"SELECT {', '.join(sel)} FROM table_rows WHERE {' AND '.join(where_parts)}"
    if group_by_exprs:
        sql += " GROUP BY " + ", ".join(group_by_exprs)
    if order_by:
        ob = order_by[0]
        # `column` here should already be a safe identifier (quoted or validated)
        sql += f" ORDER BY {ob['column']} {ob.get('dir', 'desc').upper()}"
    if limit:
        sql += f" LIMIT {limit}"
    return sql
