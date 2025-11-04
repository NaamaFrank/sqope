

CLASSIFY_PROMPT = """Classify this question into exactly one of these types:
- text: descriptive queries without numeric focus (e.g. "What is our business strategy?", "Describe our products", "Who are our competitors?")
- analytical: queries about numbers, statistics, calculations, comparisons (e.g. "What was revenue in Q4?", "Show top 5 products", "Calculate growth rate")
- hybrid: queries requesting both analysis AND explanation (e.g. "Why did revenue drop in Q4?", "Explain the sales trends", "What insights can you derive from the Q4 numbers?", "How was the q4 quarter report so far for novatech?")

Respond with ONLY the type (analytical/hybrid/text).

Question: {q}
Type:"""

PLAN_SCHEMA = """You are a planner over JSON rows.
Return STRICT JSON using ONLY column IDs.

Available schema (IDs, names, kinds) and tiny samples:
{schema_json}

Return JSON keys:
table: {{file_key: string, table_index: int}}
filters: array of {{"col_id": integer, "op": one of [">=", "<=", ">", "<", "=", "!=","in","between","contains"], "value": string or [string,string]}}
group_by: array of integers
aggregates: array of {{"func": one of ["sum","avg","count","min","max"], "col_id": integer, "as": string}}
order_by: optional array of {{"col_id": integer, "dir": "asc"|"desc"}}
limit: optional int

Rules:
- Use ONLY provided column IDs. Do NOT invent columns.
- Aggregations (sum/avg/min/max) MUST use numeric IDs.
- Temporal filters (year/quarter/time) MUST use temporal IDs.
- If unsure, OMIT filters rather than guessing.
- DO NOT put final numbers in the JSON; we will compute.

Question: {q}
STRICT JSON:
"""


def classify_prompt(q: str) -> str:
	return CLASSIFY_PROMPT.format(q=q)


def plan_prompt(schema_json: str, q: str) -> str:
	return PLAN_SCHEMA.format(schema_json=schema_json, q=q)

__all__ = ["CLASSIFY_PROMPT", "PLAN_SCHEMA", "classify_prompt", "plan_prompt"]
