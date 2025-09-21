# ingestion/opensearch_retrieval.py
from opensearchpy import OpenSearch
import os
from ingestion.embeddings import embed_texts

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "http://localhost:9200")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "admin")

CHUNKS_INDEX = "paper_chunks"

client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=OPENSEARCH_HOST.startswith("https"),
    verify_certs=False,
)

def search_vector(query_text, top_k=10):
    try:
        query_emb = embed_texts([query_text])[0]
    except Exception as e:
        # embedding failed → log + fallback empty
        print(f"[WARN] Embedding failed for query: {e}")
        return []
    
    body = {
        "size": top_k,
        "query": {
            "knn": {
                "embedding": {
                    "vector": query_emb,
                    "k": top_k
                }
            }
        }
    }
    res = client.search(index=CHUNKS_INDEX, body=body)
    return [hit["_source"] for hit in res["hits"]["hits"]]

def search_bm25_chunks(query_text, top_k=10):
    body = {
        "size": top_k,
        "query": {
            "multi_match": {
                "query": query_text,
                "fields": ["text^2", "section"]
            }
        }
    }
    res = client.search(index=CHUNKS_INDEX, body=body)
    return [hit["_source"] for hit in res["hits"]["hits"]]

def search_hybrid(query_text, top_k=10):
    bm25_res = search_bm25_chunks(query_text, top_k * 2)
    
    try:
        vector_res = search_vector(query_text, top_k * 2)
    except Exception as e:
        print(f"[WARN] Vector search failed, falling back to BM25 only: {e}")
        return bm25_res[:top_k]

    if not vector_res:
        # embedding failed inside search_vector
        return bm25_res[:top_k]

    scores = {}
    for rank, doc in enumerate(bm25_res):
        scores[doc["arxiv_id"]] = scores.get(doc["arxiv_id"], 0) + 1 / (60 + rank)
    for rank, doc in enumerate(vector_res):
        scores[doc["arxiv_id"]] = scores.get(doc["arxiv_id"], 0) + 1 / (60 + rank)

    ranked_ids = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    # fetch full docs back by ID
    id_to_doc = {d["arxiv_id"]: d for d in bm25_res + vector_res}
    return [id_to_doc[pid] for pid, _ in ranked_ids]
