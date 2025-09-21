from fastapi import FastAPI, Query
from pydantic import BaseModel
from ingestion.embeddings import embed_texts
from ingestion.opensearchRetrieval import search_bm25_chunks, search_vector, search_hybrid
import logging

logger = logging.getLogger("api")
app = FastAPI(title="Unified Search API")

class SearchRequest(BaseModel):
    query: str
    mode: str = Query("bm25", regex="^(bm25|vector|hybrid)$")
    k: int = 10

@app.get("/")
async def root():
    return {"message": "Hybrid Search API running!"}

@app.post("/search")
def search(req: SearchRequest):
    if req.mode == "bm25":
        results = search_bm25_chunks(req.query, top_k=req.k)
        return {"mode": "bm25", "results": results}

    if req.mode == "vector":
        results = search_vector(req.query, top_k=req.k)
        if not results:  # fallback if embeddings fail
            results = search_bm25_chunks(req.query, top_k=req.k)
            return {"mode": "vector-fallback-bm25", "results": results}
        return {"mode": "vector", "results": results}

    if req.mode == "hybrid":
        results = search_hybrid(req.query, top_k=req.k)
        return {"mode": "hybrid", "results": results}

    return {"error": "Invalid mode"}
