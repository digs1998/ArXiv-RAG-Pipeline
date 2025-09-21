# ingestion/opensearch_search.py
from opensearchpy import OpenSearch
import os

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "http://localhost:9200")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "admin")

PAPERS_INDEX = "papers"

client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=OPENSEARCH_HOST.startswith("https"),
    verify_certs=False,
)

# def search_papers(query_text, category=None, author=None, year_from=None, year_to=None, size=10):
#     """
#     Multi-field BM25 search with optional filters.
#     """
#     must_clauses = []
#     filter_clauses = []

#     if query_text:
#         must_clauses.append({
#             "multi_match": {
#                 "query": query_text,
#                 "fields": ["title^2", "abstract"],  # boost title relevance
#                 "type": "best_fields"
#             }
#         })

#     if category:
#         filter_clauses.append({"term": {"categories": category}})
#     if author:
#         filter_clauses.append({"term": {"authors": author}})
#     if year_from or year_to:
#         range_filter = {}
#         if year_from:
#             range_filter["gte"] = f"{year_from}-01-01"
#         if year_to:
#             range_filter["lte"] = f"{year_to}-12-31"
#         filter_clauses.append({"range": {"published_at": range_filter}})

#     search_body = {
#         "query": {
#             "bool": {
#                 "must": [
#                     {"multi_match": {"query": query_text, "fields": ["title^3","abstract","authors"]}}
#                 ],
#                 "filter": [
#                     {"range": {"published_at": {"gte": year_from, "lte": year_to}}},
#                     {"term": {"categories": category}}  # optional
#                 ]
#             }
#         },
#         "size": size
#     }

#     response = client.search(index=PAPERS_INDEX, body=search_body)
#     hits = response.get("hits", {}).get("hits", [])
#     results = [
#         {
#             "arxiv_id": hit["_source"]["arxiv_id"],
#             "title": hit["_source"]["title"],
#             "authors": hit["_source"].get("authors"),
#             "published_at": hit["_source"].get("published_at")
#         }
#         for hit in hits
#     ]
#     return results

def search_papers(query_text, category=None, year_from=None, year_to=None, size=10):
    must_clauses = []
    filter_clauses = []

    if query_text:
        must_clauses.append({
            "multi_match": {
                "query": query_text,
                "fields": ["title^3", "abstract", "authors"]
            }
        })

    if category:
        filter_clauses.append({"term": {"categories": category}})

    if year_from or year_to:
        range_filter = {"range": {"published_at": {}}}
        if year_from:
            range_filter["range"]["published_at"]["gte"] = f"{year_from}-01-01"
        if year_to:
            range_filter["range"]["published_at"]["lte"] = f"{year_to}-12-31"
        filter_clauses.append(range_filter)

    search_body = {
        "size": size,
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses   # ✅ must be a list
            }
        }
    }

    response = client.search(index=PAPERS_INDEX, body=search_body)
    return [hit["_source"] for hit in response["hits"]["hits"]]


# Example usage
if __name__ == "__main__":
    results = search_papers(
        query_text="AI in Cancer research",
        category=None,
        year_from=2020,
        year_to=2025,
        size=5
    )
    for r in results:
        print(r)
