import json
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from indexer.storage import get_vectorstore, get_engine
from app.services.llm import get_llm
from app.services.sql_templates import SELECT_COLUMN_NAMES, SELECT_TABLE_ROWS_SAMPLE, build_select_query
from app.services.prompts import plan_prompt

# ============= Small utilities =============

def _normalize(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", " ", (s or "").lower()).strip()


def _format_number(x: Any) -> str:
    try:
        f = float(x)
        return f"{int(f):,}" if f.is_integer() else f"{f:,.2f}"
    except Exception:
        return str(x)


# ============= Vectorstore table pick & schema =============
def _pick_table(question: str, file_key: Optional[str]) -> Tuple[str, int]:
    vs = get_vectorstore()
    flt = {"type": "table_schema"}
    if file_key:
        flt["file_key"] = file_key
    hits = vs.similarity_search_with_score(question, k=3, filter=flt) or []
    if not hits:
        raise ValueError("No candidate tables found.")
    doc, _ = hits[0]
    md = doc.metadata or {}
    return md["file_key"], int(md["table_index"])


def _fetch_headers(conn, file_key: str, table_index: int) -> List[str]:
    row = conn.execute(text(SELECT_COLUMN_NAMES), {"k": file_key, "i": table_index}).first()
    return list(row[0]) if row and row[0] else []


def _sample_rows(conn, file_key: str, table_index: int, columns: List[str], n: int = 8) -> List[dict]:
    rows = conn.execute(text(SELECT_TABLE_ROWS_SAMPLE), {"k": file_key, "i": table_index, "n": n}).fetchall()
    out = []
    for (data,) in rows:
        if isinstance(data, dict):
            out.append({c: str(data.get(c, "")) for c in columns})
    return out


def _trim_samples(samples: List[dict], max_chars: int = 60) -> List[dict]:
    trimmed = []
    for r in samples:
        trimmed.append({k: (v if len(v) <= max_chars else v[:max_chars] + "…") for k, v in r.items()})
    return trimmed


# ============= Column hints (embeddings + fuzzy) =============
def _fuzzy_candidates(question: str, headers: List[str], topk: int = 6) -> List[str]:
    qtok = set(_normalize(question).split())
    scored = []
    for h in headers:
        htok = set(_normalize(h).split())
        inter = len(qtok & htok)
        jacc = inter / max(1, len(qtok | htok))
        scored.append((jacc, h))
    scored.sort(reverse=True)
    return [h for sc, h in scored[:topk] if sc > 0.0]


def _column_hints(question: str, file_key: str, table_index: int, headers: List[str], topk=6) -> List[str]:
    vs = get_vectorstore()
    hints = []
    try:
        flt = {"type": "column_schema", "file_key": file_key, "table_index": table_index}
        docs = vs.similarity_search_with_score(question, k=min(topk, len(headers)), filter=flt)
        seen = set()
        for d, _ in docs or []:
            c = (d.metadata or {}).get("column_name")
            if c and c in headers and c not in seen:
                hints.append(c); seen.add(c)
    except Exception:
        pass
    if len(hints) < topk:
        extra = _fuzzy_candidates(question, [h for h in headers if h not in hints], topk=topk - len(hints))
        hints.extend(extra)
    return hints[:topk]


# ============= Type inference from samples =============
_Q_RE = re.compile(r"\bq([1-4])\b", re.I)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
DATE_LIKE_RE = re.compile(r"^\s*\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?\s*$")

def _looks_like_number(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    s2 = re.sub(r"[^\d.\-]", "", s.replace(",", ""))
    return bool(re.match(r"^-?\d+(\.\d+)?$", s2))

def _looks_like_date(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    if DATE_LIKE_RE.match(s):
        return True
    months = ("jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec")
    low = s.lower()
    return any(m in low for m in months)

def _infer_kinds(headers: List[str], samples: List[dict]) -> Dict[str, str]:
    kinds: Dict[str, str] = {}
    col_vals: Dict[str, List[str]] = {h: [] for h in headers}
    for r in samples:
        for h in headers:
            v = r.get(h, "")
            if v:
                col_vals[h].append(v)
    for h in headers:
        vals = col_vals[h][:8]
        name = h.lower()
        if (m := _Q_RE.search(name)):
            kinds[h] = f"period_q{m.group(1)}"
        elif any(t in name for t in ("date", "year", "month", "quarter", "week", "day")):
            kinds[h] = "temporal"
        elif vals and all(_looks_like_number(x) for x in vals):
            kinds[h] = "number"
        elif vals and sum(1 for x in vals if _looks_like_date(x)) >= max(2, len(vals)//2):
            kinds[h] = "temporal"
        else:
            kinds[h] = "text"
    return kinds


# ============= Intent & auto-synthesis =============
def _detect_intent(q: str) -> Optional[str]:
    ql = q.lower()
    if any(w in ql for w in ["sum", "total", "add up"]): return "sum"
    if any(w in ql for w in ["average", "avg", "mean"]): return "avg"
    if any(w in ql for w in ["maximum", "max", "highest", "top 1"]): return "max"
    if any(w in ql for w in ["minimum", "min", "lowest", "bottom 1"]): return "min"
    if any(w in ql for w in ["count", "how many", "number of"]): return "count"
    return None

def _period_tag(q: str) -> Optional[str]:
    m = _Q_RE.search(q.replace("quarter", "q"))
    return f"q{m.group(1)}" if m else None

def _choose_best_numeric(headers: List[str], kinds: Dict[str, str], q: str, period: Optional[str]) -> Optional[str]:
    qtok = set(_normalize(q).split())
    best, best_score = None, (-1, -1)
    for h in headers:
        if kinds.get(h) != "number":
            continue
        htok = set(_normalize(h).split())
        overlap = len(qtok & htok)
        bonus = 1 if (period and period in h.lower()) else 0
        score = (overlap, bonus)
        if score > best_score:
            best_score = score; best = h
    return best


# ============= LLM plan (structured), then we build SQL =============
ALLOWED_FUNCS = {"sum","avg","count","min","max"}
ALLOWED_OPS = {">=", "<=", " >", "<", "=", "!=", "in", "between", "contains"}

def _make_plan(llm, q: str, headers: List[str], hints: List[str], fk: str, ti: int, samples: List[dict], kinds: Dict[str,str]) -> Dict[str, Any]:
    idx = {i: h for i, h in enumerate(headers)}
    schema = {
        "columns": [{"id": i, "name": n, "kind": kinds.get(n, "text")} for i, n in idx.items()],
        "samples": samples,
        "suggested": hints,
    }
    numeric_ids = [i for i, h in idx.items() if kinds.get(h) == "number"]
    temporal_ids = [i for i, h in idx.items() if kinds.get(h, "").startswith("period_q") or kinds.get(h) == "temporal"]

    prompt = plan_prompt(json.dumps(schema, ensure_ascii=False), q)
    raw = (llm.invoke(prompt).content or "").strip()
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        return {"error": "no_json", "raw": raw}

    plan = json.loads(m.group(0))
    plan["table"] = {"file_key": fk, "table_index": ti}
    plan["_columns_index"] = idx
    plan["_kinds"] = kinds
    plan["_q"] = q
    plan["_numeric_ids"] = numeric_ids
    plan["_temporal_ids"] = temporal_ids
    return plan


def _id_to_col(plan: dict, cid: int) -> Optional[str]:
    return plan.get("_columns_index", {}).get(int(cid))


def _wants_scalar(q: str) -> bool:
    ql = q.lower()
    return any(w in ql for w in ["max","maximum","min","minimum","sum","total","avg","average","mean","count","top 1","largest","smallest"]) and not any(x in ql for x in [" per ", " by ", " each "])


def _validate_and_normalize(plan: Dict[str, Any], headers: List[str]) -> Tuple[bool, str]:
    if plan.get("error"):
        return False, plan["error"]

    kinds = plan.get("_kinds", {})
    numeric_ids = plan.get("_numeric_ids", [])
    temporal_ids = plan.get("_temporal_ids", [])

    # Aggregates
    for a in plan.get("aggregates", []):
        if a.get("func") not in ALLOWED_FUNCS:
            return False, "bad_agg_func"
        if a.get("func") != "count":
            if "col_id" not in a:
                return False, "missing_agg_col_id"
            if int(a["col_id"]) not in numeric_ids:
                return False, f"agg_on_non_numeric:{_id_to_col(plan, a['col_id'])}"
            col = _id_to_col(plan, a["col_id"])
            if not col:
                return False, f"bad_agg_col_id:{a['col_id']}"
            a["column"] = col
        else:
            a["column"] = None
        a["as"] = a.get("as") or (f"{a['func']}_{a['column'] or 'rows'}")

    # Group-by → map ids
    mapped_gb = []
    for gid in plan.get("group_by", []):
        col = _id_to_col(plan, gid)
        if col:
            mapped_gb.append(col)
    plan["group_by"] = mapped_gb

    # Filters
    norm_filters = []
    for f in plan.get("filters", []):
        if f.get("op") not in ALLOWED_OPS:
            return False, "bad_filter_op"
        col = _id_to_col(plan, f.get("col_id"))
        if not col:
            continue
        # temporal guardrail
        val = f.get("value")
        is_temporal_val = False
        if isinstance(val, list):
            is_temporal_val = any(bool(YEAR_RE.search(str(v))) or bool(_Q_RE.search(str(v).replace("quarter","q"))) for v in val)
        else:
            s = str(val or "")
            is_temporal_val = bool(YEAR_RE.search(s)) or bool(_Q_RE.search(s.replace("quarter","q")))
        if is_temporal_val and int(f["col_id"]) not in temporal_ids:
            continue
        f["column"] = col
        norm_filters.append(f)
    plan["filters"] = norm_filters

    # Scalar ask → no group_by, order by first agg desc, limit 1
    if plan.get("aggregates") and _wants_scalar(plan.get("_q", "")):
        plan["group_by"] = []
        first_alias = plan["aggregates"][0]["as"]
        plan["order_by"] = [{"column": first_alias, "dir": "desc"}]
        plan["limit"] = 1

    return True, "ok"


# ============= Building safe JSONB SQL =============
def _qid(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'

def _col_num(col: str) -> str:
    # numeric cast tolerant to commas and symbols
    return f"(REGEXP_REPLACE(REGEXP_REPLACE(data->>'{col}', '[,]', ''), '[^0-9.-]', '', 'g'))::numeric"

def _col_txt(col: str) -> str:
    return f"(data->>'{col}')"

def _build_sql(plan: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    fk = plan["table"]["file_key"]
    ti = int(plan["table"]["table_index"])
    filters = plan.get("filters", [])
    group_by = plan.get("group_by", [])
    aggs = plan.get("aggregates", [])
    order_by = plan.get("order_by", [])
    limit = int(plan.get("limit", 0) or 0)

    # Normalize order_by entries: accept either {'column': name, 'dir':...} or {'col_id': id, 'dir':...}
    norm_ob: List[Dict[str, Any]] = []
    cols_index = plan.get("_columns_index", {})
    for ob in order_by or []:
        if not isinstance(ob, dict):
            continue
        # already a column name
        if "column" in ob and isinstance(ob.get("column"), str):
            norm_ob.append({"column": ob.get("column"), "dir": ob.get("dir", "desc")})
            continue
        # map col_id -> aggregate alias if possible, else header name
        if "col_id" in ob:
            try:
                cid = int(ob.get("col_id"))
            except Exception:
                continue
            # prefer aggregate alias (if aggregate uses this col_id)
            agg_alias = None
            for a in aggs:
                try:
                    if a.get("col_id") is not None and int(a.get("col_id")) == cid:
                        agg_alias = a.get("as")
                        break
                except Exception:
                    continue
            if agg_alias:
                norm_ob.append({"column": agg_alias, "dir": ob.get("dir", "desc")})
                continue
            # fallback to header name via _columns_index
            header_name = cols_index.get(cid) if isinstance(cols_index, dict) else None
            if header_name:
                norm_ob.append({"column": header_name, "dir": ob.get("dir", "desc")})
                continue
        # ignore malformed entries
    order_by = norm_ob

    sel: List[str] = []
    gb_exprs: List[str] = []

    for g in group_by:
        expr = _col_txt(g)
        sel.append(f"{expr} AS {_qid(g)}")
        gb_exprs.append(expr)

    for a in aggs:
        func = a["func"].upper()
        alias = a["as"]
        if func == "COUNT":
            sel.append(f"COUNT(*) AS {_qid(alias)}")
        else:
            col = a["column"]
            sel.append(f"{func}({_col_num(col)}) AS {_qid(alias)}")

    if not sel:
        sel = ["COUNT(*) AS count_rows"]

    where_parts = ["file_key = :fk", "table_index = :ti"]
    params: Dict[str, Any] = {"fk": fk, "ti": ti}
    dyn: List[Any] = []

    def _op_sql(op: str) -> Optional[str]:
        return {"=": "=", "!=": "<>", ">=": ">=", "<=": "<=", ">": ">", "<": "<"}.get(op, None)

    for f in filters:
        col, op, val = f["column"], f["op"], f.get("value")
        if op in {">=", "<=", ">", "<", "=", "!="}:
            where_parts.append(f"{_col_num(col)} {_op_sql(op)} :p{len(dyn)}")
            dyn.append(val)
        elif op == "contains":
            where_parts.append(f"{_col_txt(col)} ILIKE :p{len(dyn)}")
            dyn.append(f"%{val}%")
        elif op == "in" and isinstance(val, list):
            ph = ",".join(f":p{len(dyn)+i}" for i in range(len(val)))
            where_parts.append(f"{_col_txt(col)} IN ({ph})")
            dyn.extend(val)
        elif op == "between" and isinstance(val, list) and len(val) == 2:
            where_parts.append(f"{_col_num(col)} BETWEEN :p{len(dyn)} AND :p{len(dyn)+1}")
            dyn.extend(val)

    # Build final SQL from centralized helper. Note: we pass already-built/select expressions
    # (sel) and group-by expressions (gb_exprs). The helper expects a safe, already-quoted
    # order_by column if provided; _build_sql earlier normalized order_by to use aliases/header names.
    # Ensure we quote the ORDER BY identifier here to match previous behavior.
    if order_by:
        # quote the first order_by column name for safety
        order_by_quoted = [{"column": _qid(order_by[0]["column"]), "dir": order_by[0].get("dir", "desc")}]
    else:
        order_by_quoted = None

    sql = build_select_query(sel, where_parts, gb_exprs, order_by_quoted, limit)

    for i, v in enumerate(dyn):
        params[f"p{i}"] = v
    return sql, params


def _run_sql(sql: str, params: Dict[str, Any]) -> List[dict]:
    eng = get_engine()
    with eng.begin() as c:
        return [dict(r) for r in c.execute(text(sql), params).mappings().all()]


# ============= Pretty answers =============
def _summarize_scalar(plan: dict, rows: List[dict]) -> Optional[str]:
    if not rows or not plan.get("aggregates") or plan.get("group_by"):
        return None
    a = plan["aggregates"][0]
    alias = a["as"]
    val = rows[0].get(alias)
    if val is None:
        return None
    pretty_fn = {"max": "Maximum", "min": "Minimum", "sum": "Sum", "avg": "Average", "count": "Count"}.get(a["func"], a["func"].capitalize())
    col = (a.get("column") or "rows").replace("_", " ").strip()
    return f"{pretty_fn} of {col}: {_format_number(val)}"


def _summarize_grouped(plan: dict, rows: List[dict]) -> Optional[str]:
    if not rows or not plan.get("group_by"):
        return None
    a = plan["aggregates"][0] if plan.get("aggregates") else None
    lines = []
    for r in rows[:5]:
        segs = []
        for g in plan["group_by"]:
            segs.append(f"{g.replace('_',' ').strip()}: {r.get(g)}")
        if a:
            alias = a["as"]
            segs.append(f"{a['func'].upper()} {(a.get('column') or 'rows').replace('_',' ').strip()}: {_format_number(r.get(alias))}")
        lines.append(" | ".join(segs))
    return "Top results:\n" + "\n".join(f"- {ln}" for ln in lines) if lines else None


# ============= Public entry =============
def analyze_table(question: str, file_key: Optional[str] = None) -> Dict[str, str]:
    # Choose table via embeddings
    fk, ti = _pick_table(question, file_key)

    # Headers, hints, samples, kinds
    eng = get_engine()
    with eng.begin() as conn:
        headers = _fetch_headers(conn, fk, ti)
        if not headers:
            return {"type": "analytical", "answer": "I couldn't find any columns in the selected table."}
        hints = _column_hints(question, fk, ti, headers)
        sample_cols = headers[: min(24, len(headers))]   
        samples = _trim_samples(_sample_rows(conn, fk, ti, sample_cols, n=8))
        kinds = _infer_kinds(headers, samples if samples else [{}])

    llm = get_llm()

    # Structured plan path (default & robust)
    plan = _make_plan(llm, question, headers, hints, fk, ti, samples, kinds)
    ok, reason = _validate_and_normalize(plan, headers)

    # If invalid/missing, auto-synthesize minimal plan from intent
    if (not ok) or (not plan.get("aggregates")):
        intent = _detect_intent(question)
        period = _period_tag(question)
        best = _choose_best_numeric(headers, kinds, question, period)
        plan["aggregates"] = []
        plan["group_by"] = []
        plan["filters"] = []

        if intent == "count" or not intent or not best:
            plan["aggregates"].append({"func": "count", "col_id": 0, "as": "count_rows", "column": None})
        else:
            col_id = next((i for i, h in plan["_columns_index"].items() if h == best), None)
            if col_id is not None:
                plan["aggregates"].append({"func": intent, "col_id": int(col_id), "as": f"{intent}_{best}"})
            else:
                plan["aggregates"].append({"func": "count", "col_id": 0, "as": "count_rows", "column": None})

        _validate_and_normalize(plan, headers)  # make it consistent; ignore errors now (safe fallbacks exist)

    # Build SQL & execute
    sql, params = _build_sql(plan)
    rows = _run_sql(sql, params)

    # Pretty human-only answer
    msg = _summarize_scalar(plan, rows) or _summarize_grouped(plan, rows)
    if msg:
        return {"type": "analytical", "answer": msg}

    if rows and "count_rows" in rows[0]:
        return {"type": "analytical", "answer": f"Found {rows[0]['count_rows']} rows."}

    return {"type": "analytical", "answer": "No rows matched the conditions."}
