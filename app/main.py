from fastapi import FastAPI
from app.routers.query import router as query_router
import os

app = FastAPI(title="Sqope AI")
app.include_router(query_router, prefix="", tags=["query"])

@app.get("/health")
def health():
    return {"ok": True}
