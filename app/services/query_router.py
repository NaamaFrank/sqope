from indexer.storage import get_vectorstore, init_db
from app.services.llm import get_llm
from app.services.table_analytics import analyze_table
from app.services.prompts import classify_prompt

import re


def detect_type(q: str) -> str:
    """
    Fast rule-based classifier with LLM fallback that returns one of: 'analytical', 'hybrid', or 'text'.
    First tries quick pattern matching, then falls back to AI for ambiguous cases.
    """
    if not q or not q.strip():
        return "text"

    ql = q.lower()

    # Fast rule-based detection first
    agg_words = {
        "sum", "total", "average", "avg", "mean", "median", "count", "max", "min",
        "top", "rank", "percentage", "percent", "%", "rate", "growth", "change",
    }
    explain_words = {"explain", "why", "insight", "insights", "interpret", "reason", "highlight", "analysis"}
    compare_words = {"between", "vs", "versus", "compare", "difference", "higher", "lower", "than"}
    analytic_phrases = {"how many", "what is the average", "what's the average", "how much", "calculate", "compute", "what is the total"}

    # Quick checks for clear indicators
    number_like = bool(re.search(r"\b\d+(?:[\.,]\d+)?(%|\b)", ql))
    quarter_like = bool(re.search(r"\bq[1-4]\b|quarter\s*\d\b", ql))
    topk_like = bool(re.search(r"\btop\s+\d+\b", ql))

    has_agg = any(w in ql for w in agg_words) or any(p in ql for p in analytic_phrases)
    has_compare = any(w in ql for w in compare_words)
    has_explain = any(w in ql for w in explain_words) or ql.strip().startswith("why") or ql.strip().startswith("explain")

    # Clear rule matches - no need for LLM
    if has_agg or has_compare or number_like or quarter_like or topk_like:
        if has_explain:
            print("[DEBUG] Rule-based classification: hybrid")
            return "hybrid"
        print("[DEBUG] Rule-based classification: analytical")
        return "analytical"

    if has_explain and ("insight" in ql or "interpret" in ql):
        print("[DEBUG] Rule-based classification: hybrid")
        return "hybrid"

    # If no clear rule match, fall back to LLM for more nuanced cases
    print("[DEBUG] No clear rule match, using LLM fallback...")
    
    llm = get_llm()
    prompt = classify_prompt(q)

    resp = llm.invoke(prompt)
    result = resp.content.strip().lower()
    print(f"[DEBUG] LLM classification result: {result}")
    
    # Normalize response to one of our three types
    detected_type = "text"  # Default fallback
    if "hybrid" in result:
        detected_type = "hybrid"
    elif "analytical" in result:
        detected_type = "analytical"
    
    print(f"[QUERY TYPE DETECTED] Question type: {detected_type}")
    return detected_type

def answer_text(q: str, analytics: str = None) -> dict:
    vs = get_vectorstore()
    flt = {"type": "hybrid"}
    docs = vs.similarity_search(q, k=4, filter=flt)
    llm = get_llm()
    context = "\n\n".join(d.page_content for d in docs)
    prompt = f"Use the context to answer:\n\n"
    if analytics:
            prompt += f"Analytical insight:\n{analytics}\n\n"
    prompt += f"Context:\n{context}\n\nQuestion: {q}"
    resp = llm.invoke(prompt)
    return {"type": "text", "answer": resp.content}

def answer_query(q: str) -> dict:
    init_db()
    t = detect_type(q)
    if t == "text":
        return answer_text(q)
    if t == "analytical":
        a = analyze_table(q)
        return a
    # hybrid: run both
    table_part = analyze_table(q)
    text_part = answer_text(q, table_part['answer'])
    return {"type": "hybrid", "answer": text_part['answer']}
