from opensearchpy import OpenSearch, helpers
import logging
import os
import time

logger = logging.getLogger("opensearch")

class OpenSearchClient:
    def __init__(self, max_retries: int = 30, retry_delay: int = 2):
        """
        Connects to OpenSearch, supports:
        - Docker internal (hostname: opensearch)
        - Local dev (localhost)
        - Remote cluster (full URL)
        """
        # Read host/port/env
        host_env = os.getenv('OPENSEARCH_HOST', 'opensearch')
        port_env = int(os.getenv('OPENSEARCH_PORT', 9200))
        protocol = os.getenv('OPENSEARCH_PROTOCOL', 'http')  # http or https

        # Determine if host_env is a full URL
        if host_env.startswith("http://") or host_env.startswith("https://"):
            self.host = host_env  # full URL, use as-is
            self.client = OpenSearch([self.host], verify_certs=False, use_ssl=self.host.startswith("https"))
        else:
            self.host = f"{protocol}://{host_env}:{port_env}"
            self.client = OpenSearch(
                hosts=[{"host": host_env, "port": port_env}],
                http_compress=True,
                use_ssl=(protocol == "https"),
                verify_certs=False
            )

        # Wait for connection
        self._wait_for_connection(max_retries, retry_delay)

    def _wait_for_connection(self, max_retries=30, delay=2):
        """Retry until OpenSearch is ready."""
        logger.info(f"🔄 Connecting to OpenSearch at {self.host}...")
        for attempt in range(max_retries):
            try:
                info = self.client.info(timeout=10)
                health = self.client.cluster.health(timeout=10)
                state = health.get('status', 'red')
                if state in ['green', 'yellow']:
                    logger.info(f"✅ OpenSearch ready (state: {state}) after {attempt + 1} attempts!")
                    return
                else:
                    logger.warning(f"ℹ️ OpenSearch state: {state}, retrying...")
            except Exception as e:
                logger.warning(f"❌ Connection attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                sleep_time = min(delay * (2 ** min(attempt, 4)), 30)
                time.sleep(sleep_time)
        logger.warning(f"⚠️ OpenSearch not fully ready after {max_retries} attempts.")

    def create_index(self):
        """Create BM25 + vector search indices"""
        # Paper metadata index
        papers_index = "arxiv_papers_index"
        if not self.client.indices.exists(index=papers_index):
            mappings = {
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "text_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "stop", "snowball"]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "arxiv_id": {"type": "keyword"},
                        "title": {"type": "text", "analyzer": "text_analyzer"},
                        "abstract": {"type": "text", "analyzer": "text_analyzer"},
                        "raw_text": {"type": "text", "analyzer": "text_analyzer"},
                        "authors": {"type": "text", "analyzer": "text_analyzer"},
                        "categories": {"type": "keyword"},
                        "published_date": {"type": "date"},
                        "embedding": {"type": "knn_vector", "dimension": 768}
                    }
                }
            }
            self.client.indices.create(index=papers_index, body=mappings)
            print(f"Created index: {papers_index}")

        # Chunks index for semantic search
        chunks_index = "arxiv_chunks_index"
        if not self.client.indices.exists(index=chunks_index):
            chunk_mappings = {
                "settings": {
                    "index": {
                        "knn": True   # ✅ required for vector search
                    },
                    "analysis": {
                        "analyzer": {
                            "text_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "stop", "snowball"]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "arxiv_id": {"type": "keyword"},
                        "chunk_text": {"type": "text", "analyzer": "text_analyzer"},
                        "section": {"type": "keyword"},
                        "embedding": {"type": "knn_vector", "dimension": 768},  # ✅ ANN ready
                        "paper_id": {"type": "integer"},
                        "chunk_idx": {"type": "integer"}
                    }
                }
            }

            self.client.indices.create(index=chunks_index, body=chunk_mappings)
            print(f"Created index: {chunks_index}")


    def ensure_indices(self):
        """Alias for create_index - for backward compatibility"""
        return self.create_index()

    def index_paper_meta(self, paper_meta):
        """Index paper metadata for BM25 search (NEW METHOD)"""
        index_name = "arxiv_papers_index"
        doc_id = paper_meta.get('arxiv_id')
        
        if not doc_id:
            print("Warning: No arxiv_id in paper metadata")
            return
        
        # Ensure required fields exist
        doc = {
            "arxiv_id": doc_id,
            "title": paper_meta.get('title', ''),
            "authors": paper_meta.get('authors', ''),
            "abstract": paper_meta.get('abstract', ''),
            "published_date": paper_meta.get('published_date', ''),
            "pdf_url": paper_meta.get('pdf_url', ''),
            "categories": paper_meta.get('categories', [])
        }
        
        # Idempotent indexing (updates if exists)
        self.client.index(index=index_name, id=doc_id, body=doc)
        print(f"Indexed paper metadata: {doc_id}")

    def index_paper(self, paper, chunks=None, embeddings=None):
        """Enhanced version - indexes metadata + optional chunks (your existing method, enhanced)"""
        # Index paper metadata
        self.index_paper_meta(paper)
        
        # Index chunks if provided
        if chunks and embeddings and len(chunks) == len(embeddings):
            self.index_chunks_bulk([{
                "arxiv_id": paper['arxiv_id'],
                "chunk_text": chunk,
                "embedding": embedding,
                "paper_id": paper.get('paper_id', 0)
            } for chunk, embedding in zip(chunks, embeddings)])
        elif chunks:
            print("Warning: Chunks provided without embeddings - skipping chunk indexing")

    def index_chunks_bulk(self, chunk_docs):
        """Bulk index chunks with embeddings (NEW METHOD)"""
        if not chunk_docs:
            return
        
        index_name = "arxiv_chunks_index"
        actions = []
        
        for doc in chunk_docs:
            action = {
                "_index": index_name,
                "_source": {
                    "arxiv_id": doc.get('arxiv_id'),
                    "chunk_text": doc.get('text', doc.get('chunk_text', '')),
                    "section": doc.get('section', 'unknown'),
                    "embedding": doc.get('embedding'),
                    "paper_id": doc.get('paper_id', 0),
                    "chunk_idx": doc.get('chunk_idx', 0)
                }
            }
            actions.append(action)
        
        # Use bulk API for efficiency
        if actions:
            helpers.bulk(self.client, actions)
            print(f"Bulk indexed {len(actions)} chunks")

    def search(self, query: str, mode: str = "bm25", size: int = 10, **kwargs):
        """Basic search method for integration with QuerySearch (NEW METHOD)"""
        from ingestion.opensearchRetrieval import QuerySearch
        search_engine = QuerySearch(self)
        return search_engine.search(query=query, mode=mode, size=size, **kwargs)

    def get_index_stats(self):
        """Get index statistics for monitoring"""
        try:
            papers_stats = self.client.indices.stats(index="arxiv_papers_index")
            chunks_stats = self.client.indices.stats(index="arxiv_chunks_index")
            return {
                "papers_count": papers_stats.get('_all', {}).get('primaries', {}).get('docs', {}).get('count', 0),
                "chunks_count": chunks_stats.get('_all', {}).get('primaries', {}).get('docs', {}).get('count', 0)
            }
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {"papers_count": 0, "chunks_count": 0}