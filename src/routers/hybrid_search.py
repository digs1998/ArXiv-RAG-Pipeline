"""Ask endpoint for BM25 search using OpenSearch."""

import logging
from fastapi import FastAPI, APIRouter, HTTPException

from src.schemas.api.search import SearchHit, SearchRequest, SearchResponse, HybridSearchRequest
from src.dependencies import EmbeddingsDep, OpenSearchDep
from src.config import get_settings
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

app = FastAPI(title="Hybrid Search API", version="0.1.0")
# --- Router definition ---
router = APIRouter(prefix="/hybrid-search", tags=["hybrid-search"])

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
    app.state.embeddings_service = EmbeddingsDep(
        api_key=os.getenv('JINA_API_KEY')
    )

@router.post("/", response_model=SearchResponse)
async def hybrid_search(
    request: HybridSearchRequest, opensearch_client: OpenSearchDep, embeddings_service: EmbeddingsDep
) -> SearchResponse:
    """
    Hybrid search endpoint supporting multiple search modes.
    """
    try:
        if not opensearch_client.health_check():
            raise HTTPException(status_code=503, detail="Search service is currently unavailable")

        query_embedding = None
        if request.use_hybrid:
            try:
                query_embedding = await embeddings_service.embed_query(request.query)
                logger.info("Generated query embedding for hybrid search")
            except Exception as e:
                logger.warning(f"Failed to generate embeddings, falling back to BM25: {e}")
                query_embedding = None

        logger.info(f"Hybrid search: '{request.query}' (hybrid: {request.use_hybrid and query_embedding is not None})")

        results = opensearch_client.search_unified(
            query=request.query,
            query_embedding=query_embedding,
            size=request.size,
            from_=request.from_,
            categories=request.categories,
            latest_papers=request.latest_papers,
            use_hybrid=request.use_hybrid,
            min_score=request.min_score,
        )

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
                    chunk_text=hit.get("chunk_text"),
                    chunk_id=hit.get("chunk_id"),
                    section_name=hit.get("section_name"),
                )
            )

        search_response = SearchResponse(
            query=request.query,
            total=results.get("total", 0),
            hits=hits,
            size=request.size,
            **{"from": request.from_},
            search_mode="hybrid" if (request.use_hybrid and query_embedding) else "bm25",
        )

        logger.info(f"Search completed: {search_response.total} results returned")
        return search_response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Hybrid search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

app.include_router(router, prefix="/api/v1")
