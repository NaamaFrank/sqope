import os
import io
import csv
import re
from typing import List, Optional, Tuple
from collections.abc import Mapping

import numpy as np 
from PIL import Image  

# --- Docling imports (explicit and robust) --------------------------------------
try:
    from docling.document_converter import DocumentConverter, PdfFormatOption, InputFormat
except ImportError:
    from docling.document_converter import DocumentConverter
    PdfFormatOption = None
    InputFormat = None

# Import pipeline options and OCR/table options if available
try:
    from docling.pipeline.options import PdfPipelineOptions, RapidOcrOptions, TesseractOcrOptions, TableStructureOptions, TableFormerMode
except ImportError:
    PdfPipelineOptions = None
    RapidOcrOptions = None
    TesseractOcrOptions = None
    TableStructureOptions = None
    TableFormerMode = None



def _log_doc_structure(doc) -> None:
    """
    Log a compact view of the converted document to help debugging.
    Only runs at DEBUG level.
    """
    try:
        doc_type = type(doc).__name__
        pages_attr = getattr(doc, "pages", None) or []
        if isinstance(pages_attr, Mapping):
            page_items = list(pages_attr.items())
        else:
            page_items = list(enumerate(pages_attr))
        print(f"[Docling] Document type: {doc_type}, pages: {len(page_items)}")
        max_pages = min(2, len(page_items))
        for i in range(max_pages):
            key, page = page_items[i]
            attrs = []
            for attr in ("paragraphs", "blocks", "elements", "lines", "tables", "ocr_text", "text", "children"):
                if hasattr(page, attr):
                    val = getattr(page, attr)
                    sample = None
                    if isinstance(val, str):
                        sample = val[:100]
                    elif isinstance(val, (list, tuple)) and val:
                        if isinstance(val[0], str):
                            sample = val[0][:100]
                        else:
                            t = getattr(val[0], "text", None) or getattr(val[0], "content", None)
                            if isinstance(t, str):
                                sample = t[:100]
                    tname = type(val).__name__ if val is not None else "None"
                    attrs.append(f"{attr}={tname} sample={repr(sample)}")
            print(f"[Docling] Page {i} ({key}): {', '.join(attrs) or '(no known attrs)'}")
    except Exception:
        print("[Docling] Could not inspect document structure.")


def _markdown_to_paragraphs(md: str) -> List[dict]:
    """
    Split Markdown into paragraphs (blank-line separated).
    Simple and robust.
    """
    if not md:
        return []
    lines = [ln.rstrip() for ln in md.splitlines()]
    paragraphs = []
    buf: List[str] = []

    def flush():
        if buf:
            text = "\n".join(buf).strip()
            if text:
                paragraphs.append({"text": text, "meta": {}})
            buf.clear()

    for ln in lines:
        if ln.strip() == "":
            flush()
        else:
            buf.append(ln)
    flush()
    return paragraphs


def _extract_markdown_tables(md: str) -> List[dict]:
    """
    Extract GitHub-style pipe tables from Markdown as a fallback when doc.tables is empty.

    Matches blocks like:
        | Col A | Col B |
        | ----- | ----: |
        |  v1   |   v2  |

    Returns [{'headers': [...], 'rows': [ {h: v, ...}, ...], 'meta': {}}...]
    """
    if not md:
        return []

    # Find candidate table blocks: header row with pipes, a delimiter row of dashes/colons,
    # then one or more body rows that start/end with pipes.
    table_blocks: List[str] = []
    lines = md.splitlines()
    i = 0
    while i < len(lines) - 1:
        header = lines[i].strip()
        sep = lines[i + 1].strip()
        if header.startswith("|") and header.endswith("|") and re.search(r"^\s*\|?\s*[:\-| ]+\|?\s*$", sep):
            # collect this table block
            blk = [header, sep]
            i += 2
            while i < len(lines) and lines[i].strip().startswith("|"):
                blk.append(lines[i].rstrip())
                i += 1
            table_blocks.append("\n".join(blk))
            continue
        i += 1

    def split_pipe_row(row: str) -> List[str]:
        # split on '|' but ignore first/last empty due to leading/ending pipes
        parts = [c.strip() for c in row.strip().strip("|").split("|")]
        return parts

    out: List[dict] = []
    for blk in table_blocks:
        blk_lines = [ln for ln in blk.splitlines() if ln.strip()]
        if len(blk_lines) < 2:
            continue
        headers = split_pipe_row(blk_lines[0])
        # skip sep row (blk_lines[1])
        rows = []
        for body in blk_lines[2:]:
            cols = split_pipe_row(body)
            row = {}
            for idx, h in enumerate(headers):
                row[h] = cols[idx] if idx < len(cols) else None
            rows.append(row)
        if headers and rows:
            out.append({"headers": headers, "rows": rows, "meta": {}})
    return out


def _parse_csv_table(csv_text: str) -> Tuple[List[str], List[dict]]:
    """
    Parse a CSV table text into headers/rows.
    """
    if not csv_text:
        return [], []
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return [], []
    headers = [h.strip() for h in rows[0]]
    out_rows = []
    for r in rows[1:]:
        row = {}
        for i, h in enumerate(headers):
            row[h] = r[i].strip() if i < len(r) else None
        out_rows.append(row)
    return headers, out_rows

def _table_to_structured(tbl) -> Tuple[Optional[List[str]], Optional[List[dict]]]:
    """
    Convert a Docling table object to (headers, rows) robustly across versions.

    Tries, in order:
      1) pandas (to_pandas)
      2) explicit columns/rows
      3) header/body split (header_rows/body_rows)
      4) flat cells with coordinates (cells/cells_flat, cell.row, cell.col, cell.text/content/value)
      5) export helpers (csv / markdown / html) as last resort
    Returns (None, None) if nothing works.
    """
    # 0) Quick helper to get a string value from a cell-like object
    def _cell_val(c):
        for attr in ("text", "content", "value", "plain_text", "ocr_text"):
            v = getattr(c, attr, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
        if isinstance(c, str):
            return c.strip()
        try:
            s = str(c)
            return s.strip()
        except Exception:
            return None

    # 1) pandas path
    to_pandas = getattr(tbl, "to_pandas", None)
    if callable(to_pandas):
        try:
            df = to_pandas()
            if getattr(df, "empty", False) is False:
                headers = [str(h) for h in list(df.columns)]
                rows = [ {str(h): (None if pd_val is None else str(pd_val)) for h, pd_val in r.items()} 
                         for r in df.to_dict(orient="records") ]
                return headers, rows
        except Exception:
            pass

    # 2) explicit columns/rows
    cols = getattr(tbl, "columns", None)
    trs  = getattr(tbl, "rows", None)
    if cols is not None and trs is not None:
        try:
            headers = [ _cell_val(c) or "" for c in cols ]
            rows = []
            for r in trs:
                cells = getattr(r, "cells", None) or []
                row_dict = {}
                for i, h in enumerate(headers):
                    val = _cell_val(cells[i]) if i < len(cells) else None
                    row_dict[str(h)] = val
                rows.append(row_dict)
            # sanitize headers: ensure unique, non-empty
            if not any(h for h in headers):
                headers = [f"col_{i}" for i in range(len(rows[0]))] if rows else []
                # remap keys
                fixed_rows = []
                for r in rows:
                    fixed_rows.append({f"col_{i}": v for i, v in enumerate(r.values())})
                rows = fixed_rows
            return headers, rows
        except Exception:
            pass

    # 3) header/body split some versions expose
    header_rows = getattr(tbl, "header_rows", None) or getattr(tbl, "headers", None)
    body_rows   = getattr(tbl, "body_rows", None)   or getattr(tbl, "body", None)
    if header_rows is not None and body_rows is not None:
        try:
            # build headers from first header row
            hdr = header_rows[0] if header_rows else []
            hdr_cells = getattr(hdr, "cells", None) or hdr
            headers = [ _cell_val(c) or "" for c in hdr_cells ]

            rows = []
            for r in body_rows:
                cells = getattr(r, "cells", None) or r
                row_dict = {}
                for i, h in enumerate(headers):
                    val = _cell_val(cells[i]) if i < len(cells) else None
                    row_dict[str(h or f"col_{i}")] = val
                rows.append(row_dict)

            if not any(h for h in headers):
                headers = [f"col_{i}" for i in range(len(rows[0]))] if rows else []
                fixed_rows = []
                for r in rows:
                    fixed_rows.append({f"col_{i}": v for i, v in enumerate(r.values())})
                rows = fixed_rows
            return headers, rows
        except Exception:
            pass

    # 4) coordinate-based cells (cells / cells_flat with row/col)
    cells_attr = getattr(tbl, "cells", None) or getattr(tbl, "cells_flat", None)
    if cells_attr:
        try:
            grid: dict[tuple[int,int], str] = {}
            max_r = -1
            max_c = -1
            for c in cells_attr:
                r = getattr(c, "row", None)
                cidx = getattr(c, "col", None)
                if r is None or cidx is None:
                    continue
                txt = _cell_val(c)
                if txt is None:
                    continue
                grid[(r, cidx)] = txt
                max_r = max(max_r, r)
                max_c = max(max_c, cidx)
            if max_r >= 0 and max_c >= 0:
                # row 0 as header if it looks like header; otherwise generate col names
                first_row = [ grid.get((0, j), "") for j in range(max_c+1) ]
                header_like = any(first_row) and all(isinstance(x, str) for x in first_row)
                headers = [ (h if h else f"col_{j}") for j, h in enumerate(first_row) ] if header_like else [f"col_{j}" for j in range(max_c+1)]

                start_r = 1 if header_like else 0
                rows = []
                for i in range(start_r, max_r+1):
                    row_dict = {}
                    for j, h in enumerate(headers):
                        row_dict[h] = grid.get((i, j), None)
                    rows.append(row_dict)
                return headers, rows
        except Exception:
            pass

    # 5) export helpers fallback (csv/markdown/html)
    for meth in ("export_to_csv", "to_csv"):
        f = getattr(tbl, meth, None)
        if callable(f):
            try:
                csv_text = f()
                if csv_text:
                    return _parse_csv_table(csv_text)
            except Exception:
                pass

    # markdown/html can be parsed but is less reliable; leave to caller
    return None, None


def _extract_tables_from_doc(doc) -> List[dict]:
    """
    Extract tables robustly with debug logging about table shapes.
    """
    out: List[dict] = []
    doc_tables = getattr(doc, "tables", None) or []

    print("_extract_tables_from_doc: doc.tables=%d", len(doc_tables) if doc_tables else 0)
    for idx, tbl in enumerate(doc_tables):
        # light introspection to help diagnose
        try:
            attrs = [a for a in dir(tbl) if not a.startswith("_")]
            print("table[%d] attrs: %s", idx, ", ".join(attrs[:40]) + ("..." if len(attrs) > 40 else ""))
        except Exception:
            pass

        headers, rows = _table_to_structured(tbl)

        # last-ditch: try markdown/html parsing if headers/rows are missing
        if headers is None:
            md = None
            html = None
            for meth in ("export_to_markdown", "to_markdown"):
                f = getattr(tbl, meth, None)
                if callable(f):
                    try:
                        # Pass doc argument for export_to_markdown
                        md = f(doc=doc) if meth == "export_to_markdown" else f()
                        break
                    except Exception:
                        pass
            if md:
                # simple pipe-table parse; reuse your _extract_markdown_tables
                md_tbls = _extract_markdown_tables(md)
                if md_tbls:
                    out.extend(md_tbls)
                    continue

            for meth in ("export_to_html", "to_html"):
                f = getattr(tbl, meth, None)
                if callable(f):
                    try:
                        html = f()
                        break
                    except Exception:
                        pass
            if html:
                # very naive HTML table parse: strip tags; real parsing would use BeautifulSoup
                try:
                    import re as _re
                    rows_html = _re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=_re.I|_re.S)
                    parsed_rows = []
                    headers = []
                    for r_i, tr in enumerate(rows_html):
                        cells = _re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, flags=_re.I|_re.S)
                        cells = [ _re.sub(r"<[^>]+>", "", c).strip() for c in cells ]
                        if r_i == 0:
                            headers = [c or f"col_{j}" for j, c in enumerate(cells)]
                        else:
                            row = { headers[j] if j < len(headers) else f"col_{j}" : (cells[j] if j < len(cells) else None) for j in range(max(len(headers), len(cells))) }
                            parsed_rows.append(row)
                    if headers and parsed_rows:
                        out.append({"headers": headers, "rows": parsed_rows, "meta": {}})
                        continue
                except Exception:
                    pass

        # normal path
        if headers is not None and rows is not None:
            out.append({"headers": headers or [], "rows": rows or [], "meta": {}})
        else:
            print("table[%d]: could not extract any structured content", idx)

    return out



def _build_converter() -> DocumentConverter:
    """
    Build a DocumentConverter with explicit PDF pipeline options:
    - OCR enabled (RapidOCR by default)
    - Table structure recovery enabled (TableFormer)
    Falls back gracefully if some option classes are missing in the installed Docling version.
    """
    do_ocr = True
    use_rapidocr = True  # flip to False to prefer Tesseract 

    pdf_pipeline_kwargs = {
        "do_ocr": do_ocr,
        "do_table_structure": True,
    }

    # Table structure options if available
    if TableStructureOptions is not None:
        mode = TableFormerMode.ACCURATE if TableFormerMode is not None else None
        if mode is not None:
            pdf_pipeline_kwargs["table_structure_options"] = TableStructureOptions(mode=mode)

    # OCR options
    if do_ocr and PdfPipelineOptions is not None:
        if use_rapidocr and RapidOcrOptions is not None:
            pdf_pipeline_kwargs["ocr_options"] = RapidOcrOptions(
                lang="eng",
                force_full_page_ocr=True,
            )
        elif TesseractOcrOptions is not None:
            pdf_pipeline_kwargs["ocr_options"] = TesseractOcrOptions(
                lang="eng",
                psm=6,
                force_full_page_ocr=True,
            )

    pipeline_options = PdfPipelineOptions(**pdf_pipeline_kwargs) if PdfPipelineOptions is not None else None

    format_options = None
    if PdfFormatOption is not None and InputFormat is not None and pipeline_options is not None:
        format_options = {InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}

    if format_options is not None:
        return DocumentConverter(format_options=format_options)

    try:
        return DocumentConverter(pipeline_options=pipeline_options)  # type: ignore
    except Exception:
        print("Falling back to DocumentConverter() without explicit pipeline options.")
        return DocumentConverter()



def parse_pdf(path: str):
    """
    Convert a PDF to:
      meta: {doc_id, pages}
      paragraphs: [{'text', 'meta'}...]
      tables: [{'headers': [...], 'rows': [...], 'meta': {...}}, ...]

    Notes:
      * OCR and Table structure are enabled via Docling PDF pipeline options.
      * Paragraphs come from Markdown export for stitched reading order.
      * Tables are taken from Docling's table model; fallback to pipe-table parsing from Markdown if needed.
    """
    converter = _build_converter()

    print(f"[PDF] Converting: {path}")
    try:
        res = converter.convert(path)
    except Exception as e:
        print(f"[PDF] Conversion failed: {e}")
        raise

    # Log converter warnings/errors if present
    for attr in ("errors", "warnings"):
        val = getattr(res, attr, None)
        if val:
            print(f"[PDF] {attr.capitalize()}: {val}")

    # Docling returns a result object with `.document`
    doc = getattr(res, "document", res)

    # Debug structure (optional)
    _log_doc_structure(doc)

    # --------- Paragraphs (from Markdown) ----------
    paragraphs: List[dict] = []
    md: Optional[str] = None
    export_md = getattr(doc, "export_to_markdown", None)
    if callable(export_md):
        try:
            md = export_md(doc=doc) 
            paragraphs = _markdown_to_paragraphs(md)
        except Exception as e:
            print(f"[PDF] Markdown export failed: {e}")
            paragraphs = []
    else:
        paras = getattr(doc, "paragraphs", None) or []
        for p in paras:
            text = getattr(p, "content", None) or getattr(p, "text", None)
            if isinstance(text, str) and text.strip():
                paragraphs.append({"text": text.strip(), "meta": {}})

    # attach filepath and pagenum to paragraphs
    for p in paragraphs:
        p.setdefault("meta", {})["filepath"] = path

    # --------- Tables ----------
    tables = _extract_tables_from_doc(doc)

    # Fallback: try Markdown pipe tables if doc.tables was empty
    if not tables and md:
        md_tables = _extract_markdown_tables(md)
        if md_tables:
            print(f"Found {len(md_tables)} Markdown pipe tables as fallback.")
            tables.extend(md_tables)

    for t in tables:
        t.setdefault("meta", {})["filepath"] = path

    # --------- Meta ----------
    pages_count = 0
    try:
        pages_attr = getattr(doc, "pages", None)
        pages_count = len(pages_attr) if pages_attr is not None else 0
    except Exception:
        pages_count = 0
    meta = {"filepath": path, "pages": pages_count}

    print(f"[PDF] filepath={path} | pages={pages_count} | paragraphs={len(paragraphs)} | tables={len(tables)}")
    if paragraphs:
        print(f"[PDF] Sample paragraph: {paragraphs[0]['text'][:100]}")

    return meta, paragraphs, tables
