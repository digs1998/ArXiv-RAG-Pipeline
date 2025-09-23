from typing import List, Optional

class QueryBuilder:
    def build_bm25(self, query: str, arxiv_ids: Optional[List[str]] = None, size: int = 10):
        base_query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "abstract^2", "authors^1"],  # Boosts from examples
                    "type": "best_fields",
                    "operator": "or",
                    "fuzziness": "AUTO"
                }
            },
            "size": size,
            "_source": ["arxiv_id", "title", "authors", "abstract", "categories", "published_date"]
        }
        if arxiv_ids:
            base_query["query"] = {
                "bool": {
                    "must": [base_query["query"]],
                    "filter": {"terms": {"arxiv_id": arxiv_ids}}  # Exact filter on keyword field
                }
            }
        return base_query

    def build_knn(self, query_emb: list, size: int = 10):
        return {
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_emb,
                        "k": size
                    }
                }
            },
            "size": size
        }

    def rrf_fuse(self, bm25_hits: list, knn_hits: list, k: int = 60):
        scores = {}
        for rank, hit in enumerate(bm25_hits, 1):
            doc_id = hit['_id']
            scores[doc_id] = scores.get(doc_id, 0) + 1 / (k + rank)
        for rank, hit in enumerate(knn_hits, 1):
            doc_id = hit['_id']
            scores[doc_id] = scores.get(doc_id, 0) + 1 / (k + rank)
        fused = [{"_id": doc_id, "score": score, "_source": self._get_source(doc_id)} for doc_id, score in scores.items()]
        return sorted(fused, key=lambda x: x["score"], reverse=True)  # Normalize if needed

    def _get_source(self, doc_id):
        # Fetch source or cache; simplify by assuming hits have _source
        return {}  # Expand as needed