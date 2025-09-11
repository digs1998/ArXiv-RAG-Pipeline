# ingestion/opensearchClient.py
import os
import logging
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "http://localhost:9200")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "admin")

# Index names
PAPERS_INDEX = os.getenv("PAPERS_INDEX", "papers")
CHUNKS_INDEX = os.getenv("CHUNKS_INDEX", "paper_chunks")

# Create the OpenSearch client
client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_compress=True,
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=OPENSEARCH_HOST.startswith("https"),
    verify_certs=False,  # set to True if using trusted certs
)

def ensure_indices():
    """
    Ensure that the required indices exist in OpenSearch.
    """
    try:
        if not client.indices.exists(index=PAPERS_INDEX):
            logger.info(f"Creating index: {PAPERS_INDEX}")
            client.indices.create(
                index=PAPERS_INDEX,
                body={
                    "mappings": {
                        "properties": {
                            "arxiv_id": {"type": "keyword"},
                            "title": {"type": "text"},
                            "authors": {"type": "text"},
                            "abstract": {"type": "text"},
                            "published_date": {"type": "date"},
                            "pdf_url": {"type": "keyword"}
                        }
                    }
                }
            )

        if not client.indices.exists(index=CHUNKS_INDEX):
            logger.info(f"Creating index: {CHUNKS_INDEX}")
            
            # Try different vector field types based on OpenSearch version
            vector_mappings = [
                # First try knn_vector (OpenSearch 2.x)
                {
                    "mappings": {
                        "properties": {
                            "paper_id": {"type": "keyword"},
                            "arxiv_id": {"type": "keyword"},
                            "chunk_idx": {"type": "integer"},
                            "text": {"type": "text"},
                            "embedding": {
                                "type": "knn_vector",
                                "dimension": 768,
                                "method": {
                                    "name": "hnsw",
                                    "space_type": "cosinesimil",
                                    "engine": "lucene"
                                }
                            }
                        }
                    },
                    "settings": {
                        "index": {
                            "knn": True
                        }
                    }
                },
                # Fallback to dense_vector (older versions)
                {
                    "mappings": {
                        "properties": {
                            "paper_id": {"type": "keyword"},
                            "arxiv_id": {"type": "keyword"},
                            "chunk_idx": {"type": "integer"},
                            "text": {"type": "text"},
                            "embedding": {"type": "dense_vector", "dims": 768}
                        }
                    }
                },
                # Final fallback - no vector field
                {
                    "mappings": {
                        "properties": {
                            "paper_id": {"type": "keyword"},
                            "arxiv_id": {"type": "keyword"},
                            "chunk_idx": {"type": "integer"},
                            "text": {"type": "text"},
                            "embedding": {"type": "float", "index": False}  # Store as array of floats
                        }
                    }
                }
            ]
            
            created = False
            for i, mapping in enumerate(vector_mappings):
                try:
                    client.indices.create(index=CHUNKS_INDEX, body=mapping)
                    logger.info(f"Successfully created {CHUNKS_INDEX} index with mapping {i+1}")
                    created = True
                    break
                except Exception as e:
                    logger.warning(f"Mapping {i+1} failed: {e}")
                    if i == len(vector_mappings) - 1:  # Last attempt
                        raise
            
            if not created:
                raise Exception("Failed to create chunks index with any mapping")

    except Exception as e:
        logger.error(f"Error ensuring indices: {e}")
        raise


def index_paper_meta(meta: dict):
    """
    Index a single paper's metadata in OpenSearch.
    """
    try:
        client.index(index=PAPERS_INDEX, document=meta, id=meta["arxiv_id"])
        logger.info(f"Indexed metadata for paper {meta['arxiv_id']}")
    except Exception as e:
        logger.error(f"Error indexing paper {meta.get('arxiv_id')}: {e}")


def index_chunks_bulk(chunks: list[dict]):
    """
    Bulk index text chunks + embeddings for a paper.
    """
    try:
        actions = [
            {"index": {"_index": CHUNKS_INDEX, "_id": f"{doc['arxiv_id']}_{doc['chunk_idx']}"}}
            for doc in chunks
        ]
        docs = []
        for doc in chunks:
            # Ensure embedding is a list for compatibility
            doc_copy = doc.copy()
            if isinstance(doc_copy.get('embedding'), list):
                doc_copy['embedding'] = doc_copy['embedding']
            docs.append(doc_copy)

        # OpenSearch bulk requires alternating action and doc
        body = []
        for action, doc in zip(actions, docs):
            body.append(action)
            body.append(doc)

        resp = client.bulk(body=body)
        if resp.get("errors"):
            logger.error(f"Bulk indexing errors: {resp}")
        else:
            logger.info(f"Successfully indexed {len(chunks)} chunks")
    except Exception as e:
        logger.error(f"Error bulk indexing chunks: {e}")


def test_vector_search(query_vector, k=5):
    """
    Test vector similarity search (if k-NN is working)
    """
    try:
        search_body = {
            "size": k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_vector,
                        "k": k
                    }
                }
            }
        }
        
        response = client.search(index=CHUNKS_INDEX, body=search_body)
        return response
    except Exception as e:
        logger.warning(f"k-NN search failed, falling back to text search: {e}")
        # Fallback to regular text search
        search_body = {
            "size": k,
            "query": {
                "match_all": {}
            }
        }
        return client.search(index=CHUNKS_INDEX, body=search_body)