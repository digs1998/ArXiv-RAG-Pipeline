"""Ask endpoint for BM25 search using OpenSearch."""

import logging
from fastapi import FastAPI, APIRouter, HTTPException

from src.dependencies import OpenSearchDep
from src.schemas.api.search import SearchHit, SearchRequest, SearchResponse
from src.config import get_settings

logger = logging.getLogger(__name__)

app = FastAPI(title="Search API", version="0.1.0")
# --- Router definition ---
router = APIRouter(prefix="/search", tags=["search"])

@router.get("/")
async def ping():
    """Simple GET endpoint for health/testing."""
    return {"status": "ok", "message": "search service is up"}

@app.on_event("startup")
async def startup_event():
    settings = get_settings()  # Load your config
    app.state.opensearch_client = OpenSearchDep(
        host=settings.opensearch.host,
        settings=settings
    )

@router.post("/", response_model=SearchResponse)
async def search_papers(request: SearchRequest, opensearch_client: OpenSearchDep) -> SearchResponse:
    """
    Search papers using BM25 scoring in OpenSearch.

    This endpoint searches across paper titles (3x boost), abstracts (2x boost),
    and authors (1x boost) using OpenSearch's BM25 algorithm for relevance scoring.
    Results can be sorted by relevance (default) or by publication date (latest_papers=true).
    """
    try:
        # Check if OpenSearch is healthy
        if not opensearch_client.health_check():
            raise HTTPException(status_code=503, detail="Search service is currently unavailable")

        # Perform search with filters
        logger.info(f"Searching for: {request.query} (latest_papers: {request.latest_papers})")
        results = opensearch_client.search_papers(
            query=request.query,
            size=request.size,
            from_=request.from_,
            categories=request.categories,
            latest_papers=request.latest_papers,
        )

        # Convert results to response model
        hits = []
        for hit in results.get("hits", []):
            hits.append(
                SearchHit(
                    arxiv_id=hit.get("arxiv_id", ""),
                    title=hit.get("title", ""),
                    authors=hit.get("authors"),
                    abstract=hit.get("abstract"),
                    published_date=hit.get("published_date"),
                    pdf_url=hit.get("pdf_url"),
                    score=hit.get("score", 0.0),
                    highlights=hit.get("highlights"),
                )
            )

        return SearchResponse(
                query=request.query,
                total=results.get("total", 0),
                hits=hits,
                size=request.size,
                from_=request.from_,
                error=results.get("error")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

app.include_router(router, prefix="/api/v1")
