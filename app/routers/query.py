from fastapi import APIRouter
from pydantic import BaseModel
from app.services.query_router import answer_query

router = APIRouter()

class QueryRequest(BaseModel):
    question: str

@router.post("/query")
def query(req: QueryRequest):
    """
    Unified entry point:
    - Detect type: textual / analytical / hybrid
    - Route to the right chain(s)
    - Return final answer + evidence
    """
    result = answer_query(req.question)
    return result
