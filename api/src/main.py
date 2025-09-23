# /app/src/main.py
from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import List, Optional
from ingestion.opensearchRetrieval import QuerySearch  # Your search engine
from ingestion.opensearchClient import OpenSearchClient

app = FastAPI(title="Search RAG API", version="1.0.0")

class SearchRequest(BaseModel):
    query: str = Field(..., description="Search query text")
    mode: str = Field("hybrid", description="Search mode: 'bm25', 'knn', or 'hybrid'")
    categories: Optional[List[str]] = Field(None, description="Filter by arXiv categories")
    year_from: Optional[int] = Field(None, description="Filter by publication year (from)")
    year_to: Optional[int] = Field(None, description="Filter by publication year (to)")
    latest_papers: bool = Field(False, description="Sort by recency")
    size: int = Field(10, ge=1, le=100, description="Number of results")
    from_: int = Field(0, alias="from", description="Pagination offset")



# ✅ Search response model (you probably have this)
class SearchHit(BaseModel):
    arxiv_id: str
    title: str
    authors: str
    abstract: str
    score: float
    original_score: Optional[float] = None
    search_mode: str
    section: Optional[str] = None

class SearchResponse(BaseModel):
    query: str
    total: int
    hits: List[SearchHit]
    search_mode: str
    took: float

# Initialize services
client = OpenSearchClient()
search_engine = QuerySearch(client)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        info = client.client.info()
        return {
            "status": "healthy",
            "opensearch_version": info.get("version", {}).get("number", "unknown"),
            "indices": client.get_index_stats()
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/search", response_model=SearchResponse)
async def search_papers(request: SearchRequest):  # ✅ Now properly defined!
    """Week 4 Hybrid Search Endpoint"""
    try:
        # Execute search with your Week 4 logic
        results = search_engine.search(
            query=request.query,
            mode=request.mode,
            # arxiv_ids=request.arxiv_ids,
            categories=request.categories,
            year_from=request.year_from,
            year_to=request.year_to,
            size=request.size
            # pass from_=request.from_ if you want pagination
        )
        
        # Transform results for API response
        hits = []
        for hit in results['hits']['hits']:
            source = hit.get('_source', {})
            main_score = hit.get('_score', 0.0)
            original_score = hit.get('original_score')
            
            # Handle both paper and chunk results
            title = source.get('title') or source.get('chunk_text', '')[:100] + "..."
            abstract = source.get('abstract') or source.get('chunk_text', '')[:200] + "..."
            
            hits.append(SearchHit(
                arxiv_id=source.get('arxiv_id', ''),
                title=title,
                authors=source.get('authors', 'N/A'),
                abstract=abstract,
                score=round(main_score, 4),
                original_score=round(original_score, 4) if original_score else None,
                search_mode=request.mode,
                section=source.get('section')
            ))
        
        return SearchResponse(
            query=request.query,
            total=results['hits']['total']['value'],
            hits=hits,
            search_mode=results.get('search_mode', request.mode),
            took=round(results.get('took', 0) * 1000, 2),
            # optionally add from_ if you extend your SearchResponse model
        )

        
    except Exception as e:
        print(f"Search error: {e}")
        return SearchResponse(
            query=request.query,
            total=0,
            hits=[],
            search_mode=request.mode,
            took=0.0
        )