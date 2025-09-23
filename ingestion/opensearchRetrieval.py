from opensearchpy import OpenSearch, helpers
from ingestion.embeddings import embed_texts 
from typing import List, Optional, Dict
import time
import os

class QuerySearch:
    def __init__(self, opensearch_client):
        self.client = opensearch_client

    def _bm25_search(self, query: str, arxiv_ids: Optional[List[str]] = None, 
                    categories: Optional[List[str]] = None, year_from: Optional[int] = None,
                    year_to: Optional[int] = None, size: int = 10) -> Dict:
        """Pure BM25 search - returns OpenSearch _score"""
        start_time = time.time()
        
        # Build query
        base_query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "abstract^2", "authors^1"],
                    "type": "best_fields",
                    "operator": "or",
                    "fuzziness": "AUTO"
                }
            },
            "size": size,
            "_source": ["arxiv_id", "title", "authors", "abstract", "categories", "published_date"]
        }
        
        # Add filters
        filter_clauses = []
        if arxiv_ids:
            filter_clauses.append({"terms": {"arxiv_id": arxiv_ids}})
        if categories:
            filter_clauses.append({"terms": {"categories": categories}})
        if year_from or year_to:
            date_range = {}
            if year_from:
                date_range["gte"] = f"{year_from}-01-01"
            if year_to:
                date_range["lte"] = f"{year_to}-12-31"
            filter_clauses.append({"range": {"published_date": date_range}})
        
        if filter_clauses:
            base_query["query"] = {
                "bool": {
                    "must": [base_query["query"]],
                    "filter": filter_clauses
                }
            }
        
        # Execute search
        response = self.client.client.search(index="arxiv_papers_index", body=base_query)
        
        # Preserve OpenSearch _score for each hit
        hits = []
        for hit in response['hits']['hits']:
            hits.append({
                '_id': hit['_id'],
                '_score': hit['_score'],  # BM25 score (e.g., 8.234)
                '_source': hit['_source']
            })
        
        took = time.time() - start_time
        return {
            'took': took,
            'hits': {
                'total': {'value': response['hits']['total']['value']},
                'hits': hits
            },
            'search_mode': 'bm25'
        }


    def _knn_search(self, query: str, size: int = 10) -> Dict:
        """Pure semantic search using your BATCH embedding function"""
        start_time = time.time()
        
        # ✅ Use your excellent batch function!
        try:
            query_embeddings = embed_texts([query])  # Pass as list
            if not query_embeddings or not query_embeddings[0]:
                raise Exception("Failed to generate query embedding")
            query_embedding = query_embeddings[0]  # Extract single embedding
        except Exception as e:
            raise Exception(f"Failed to generate embedding: {e}")
        
        # KNN query
        knn_body = {
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_embedding,
                        "k": size
                    }
                }
            },
            "size": size,
            "_source": ["arxiv_id", "chunk_text", "section"]
        }
        
        response = self.client.client.search(index="arxiv_chunks_index", body=knn_body)
        
        # Preserve cosine similarity scores
        hits = []
        for hit in response['hits']['hits']:
            hits.append({
                '_id': hit['_id'],
                '_score': hit['_score'],  # Cosine similarity (e.g., 0.8942)
                '_source': hit['_source']
            })
        
        took = time.time() - start_time
        return {
            'took': took,
            'hits': {
                'total': response['hits']['total'],
                'hits': hits
            },
            'search_mode': 'knn'
        }

    def _rrf_fuse(self, bm25_hits: List[Dict], knn_hits: List[Dict], k: int = 60) -> List[Dict]:
        """Reciprocal Rank Fusion - Week 4 style"""
        scores = {}
        
        # BM25 contributions
        for rank, hit in enumerate(bm25_hits, 1):
            doc_id = hit['_id']
            rrf_score = 1 / (k + rank)
            scores[doc_id] = scores.get(doc_id, 0) + rrf_score
        
        # KNN contributions
        for rank, hit in enumerate(knn_hits, 1):
            doc_id = hit['_id']
            rrf_score = 1 / (k + rank)
            scores[doc_id] = scores.get(doc_id, 0) + rrf_score
        
        # Sort by RRF score
        ranked_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        # Create fused hits
        fused_hits = []
        for doc_id, rrf_score in ranked_docs[:10]:
            best_doc = None
            best_original_score = 0
            
            # Find best document from either source
            for hit in bm25_hits + knn_hits:
                if hit['_id'] == doc_id and hit['_score'] > best_original_score:
                    best_doc = hit
                    best_original_score = hit['_score']
            
            if best_doc:
                fused_hits.append({
                    '_id': doc_id,
                    '_score': round(rrf_score, 4),  # RRF score
                    'original_score': round(best_original_score, 4),
                    '_source': best_doc['_source']
                })
        
        return fused_hits

    def _hybrid_search(self, query: str, arxiv_ids: Optional[List[str]] = None, 
                      categories: Optional[List[str]] = None, size: int = 10) -> Dict:
        """Hybrid search with RRF fusion"""
        start_time = time.time()
        
        # Get both types of results
        bm25_results = self._bm25_search(query, arxiv_ids, categories, size=size)
        knn_results = self._knn_search(query, size=size)
        
        # Fuse with RRF
        fused_hits = self._rrf_fuse(bm25_results['hits']['hits'], knn_results['hits']['hits'])
        
        took = time.time() - start_time
        return {
            'took': took,
            'hits': {
                'total': {'value': len(fused_hits)},
                'hits': fused_hits
            },
            'search_mode': 'hybrid'
        }

    def search(self, query: str, mode: str = "hybrid", arxiv_ids: Optional[List[str]] = None,
          categories: Optional[List[str]] = None, year_from: Optional[int] = None,
          year_to: Optional[int] = None, size: int = 10, from_: int = 0) -> Dict:
        """Main search dispatcher with normalized output"""
        try:
            if mode == "bm25":
                results = self._bm25_search(query, arxiv_ids, categories, year_from, year_to, size)
            elif mode == "knn":
                results = self._knn_search(query, size)
            elif mode == "hybrid":
                results = self._hybrid_search(query, arxiv_ids, categories, size)
            else:
                raise ValueError(f"Unknown search mode: {mode}")
        except Exception as e:
            print(f"Search failed in {mode} mode, falling back to BM25: {e}")
            results = self._bm25_search(query, arxiv_ids, categories, year_from, year_to, size)

        # Always return formatted response
        return self._format_response(query, results, size=size, from_=from_)

    
    def _format_response(self, query: str, results: Dict, size: int, from_: int) -> Dict:
        """
        Normalize OpenSearch results into the clean response schema.
        """
        total = results["hits"]["total"]["value"] if isinstance(results["hits"]["total"], dict) else results["hits"]["total"]

        hits = []
        for r in results["hits"]["hits"]:
            source = r.get("_source", {})
            hits.append({
                "arxiv_id": source.get("arxiv_id"),
                "title": source.get("title"),
                "authors": source.get("authors"),
                "abstract": source.get("abstract"),
                "score": r.get("_score", 0.0),
                "chunk_text": source.get("chunk_text"),
                "chunk_id": source.get("chunk_id", ""),
                "section_name": source.get("section", "unknown")
            })

        return {
            "query": query,
            "total": total,
            "hits": hits,
            "search_mode": results.get("search_mode", "bm25"),
            "size": size,
            "from": from_
        }
