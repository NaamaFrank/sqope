from indexer.storage import get_vectorstore, init_db
from app.services.llm import get_llm
from app.services.tables import analyze_table

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
    prompt = """Classify this question into exactly one of these types:
- analytical: queries about numbers, statistics, calculations, comparisons (e.g. "What was revenue in Q4?", "Show top 5 products", "Calculate growth rate")
- hybrid: queries requesting both analysis AND explanation (e.g. "Why did revenue drop in Q4?", "Explain the sales trends", "What insights can you derive from the Q4 numbers?")
- text: descriptive queries without numeric focus (e.g. "What is our business strategy?", "Describe our products", "Who are our competitors?")

Respond with ONLY the type (analytical/hybrid/text).

Question: {q}
Type:""".format(q=q)

    resp = llm.invoke(prompt)
    result = resp.content.strip().lower()
    print(f"[DEBUG] LLM classification result: {result}")
    
    # Normalize response to one of our three types
    if "hybrid" in result:
        return "hybrid"
    elif "analytical" in result:
        return "analytical"
    return "text"  # Default fallback

def answer_text(q: str) -> dict:
    vs = get_vectorstore()
    docs = vs.similarity_search(q, k=4)
    llm = get_llm()
    context = "\n\n".join(d.page_content for d in docs)
    prompt = f"Use the context to answer:\n\nContext:\n{context}\n\nQuestion: {q}"
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
    # hybrid: run both, then fuse
    text_part = answer_text(q)
    table_part = analyze_table(q)
    fused = f"{text_part['answer']}\n\nAnalytical insight: {table_part['answer']}"
    return {"type": "hybrid", "answer": fused, "text": text_part, "analytical": table_part}
