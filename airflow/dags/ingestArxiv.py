# airflow/dags/arxiv_daily_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# --- Ingestion imports ---
from ingestion.client.arxivClient import ArxivClient
from ingestion.db import init_db, SessionLocal, upsert_paper, Paper, Chunk
from ingestion.downloaders.pdfDownloader import PDFDownloader
from ingestion.processing import parse_and_chunk
from ingestion.embeddings import embed_texts
from ingestion.opensearchClient import ensure_indices, index_paper_meta, index_chunks_bulk
from ingestion.opensearchSearch import search_papers

logger = logging.getLogger("airflow")

# -------------------------------------------------------------------
# Default DAG arguments
# -------------------------------------------------------------------
default_args = {
    "owner": "rag_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 9),   # <-- yesterday, so it can run now
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
ARXIV_QUERY = "Explainable AI in cancer research"
YEAR_START = 2020
YEAR_END = 2025
BATCH_SIZE = 50

# -------------------------------------------------------------------
# Task functions
# -------------------------------------------------------------------
def task_init(**kwargs):
    """Initialize database and indices."""
    init_db()
    ensure_indices()


def task_fetch_metadata(**kwargs):
    client = ArxivClient()
    xml = client.fetch_metadata(query=ARXIV_QUERY, start=0, max_results=BATCH_SIZE)
    print(f"XML contents {xml}")
    papers = client.parse_metadata(xml)
    print(f"Papers {papers}")
    kwargs["ti"].xcom_push(key="papers", value=papers)


def task_store_metadata(**kwargs):
    papers = kwargs["ti"].xcom_pull(key="papers", task_ids="fetch_metadata") or []
    sess = SessionLocal()
    saved = []
    for p in papers:
        meta = {
            "arxiv_id": p["arxiv_id"],
            "title": p.get("title"),
            "authors": p.get("authors", ""),
            "abstract": p.get("abstract", ""),
            "published_date": p.get("published_year")
            and f"{p.get('published_year')}-01-01",
            "pdf_url": p.get("pdf_url"),
        }
        paper_obj = upsert_paper(sess, meta=meta, pdf_path=None)
        saved.append(
            {
                "arxiv_id": paper_obj.arxiv_id,
                "id": paper_obj.id,
                "pdf_url": paper_obj.pdf_url,
            }
        )
        index_paper_meta(meta)
    sess.commit()
    sess.close()
    kwargs["ti"].xcom_push(key="saved_papers", value=saved)


def task_download_pdfs(**kwargs):
    saved = kwargs["ti"].xcom_pull(key="saved_papers", task_ids="store_metadata") or []
    logger.info(f"Retrieved {len(saved)} saved papers: {saved}")
    
    downloader = PDFDownloader()
    import asyncio
    
    # Get the actual papers with their published years
    sess = SessionLocal()
    papers_for_download = []
    for s in saved:
        paper_obj = sess.query(Paper).filter_by(arxiv_id=s["arxiv_id"]).one_or_none()
        if paper_obj:
            # Fix: Handle datetime.date object properly
            if paper_obj.published_date:
                if isinstance(paper_obj.published_date, str):
                    year = int(paper_obj.published_date.split('-')[0])
                else:
                    # It's already a datetime.date object
                    year = paper_obj.published_date.year
            else:
                year = None
                
            papers_for_download.append({
                "arxiv_id": s["arxiv_id"], 
                "pdf_url": s["pdf_url"], 
                "published_year": year
            })
            logger.info(f"Paper {s['arxiv_id']}: year={year}, pdf_url={s['pdf_url']}")
    
    sess.close()
    
    logger.info(f"Papers being sent to downloader: {len(papers_for_download)} papers")
    logger.info(f"Year range filter: {YEAR_START} to {YEAR_END}")

    paths = asyncio.run(
        downloader.download_batch(papers_for_download, YEAR_START, YEAR_END)
    )
    
    logger.info(f"Downloaded {len(paths)} files: {[p.name for p in paths]}")
    mapping = {p.name.replace(".pdf", ""): str(p) for p in paths}
    kwargs["ti"].xcom_push(key="pdf_paths", value=mapping)

def clean_text_for_db(text):
    """Clean text to remove characters that cause database issues."""
    if not text:
        return text
    
    # Remove null bytes that cause PostgreSQL issues
    cleaned = text.replace('\x00', '')
    
    # Optionally remove other problematic characters
    # cleaned = ''.join(char for char in cleaned if ord(char) >= 32 or char in '\n\r\t')
    
    return cleaned


def task_parse_chunk_embed_index(**kwargs):
    saved = kwargs["ti"].xcom_pull(key="saved_papers", task_ids="store_metadata") or []
    pdf_map = kwargs["ti"].xcom_pull(key="pdf_paths", task_ids="download_pdfs") or {}
    sess = SessionLocal()

    for s in saved:
        arxiv_id = s["arxiv_id"]
        paper_obj = sess.query(Paper).filter_by(arxiv_id=arxiv_id).one_or_none()
        if not paper_obj:
            logger.warning(f"Paper {arxiv_id} not found in DB")
            continue

        pdf_path = pdf_map.get(arxiv_id)
        if not pdf_path:
            logger.warning(f"No PDF for {arxiv_id}, skipping")
            continue

        chunks = parse_and_chunk(pdf_path)
        if not chunks:
            continue

        # CLEAN THE CHUNKS before saving to database
        cleaned_chunks = [clean_text_for_db(chunk) for chunk in chunks]

        # Save cleaned chunks in DB
        for idx, txt in enumerate(cleaned_chunks):
            if txt and txt.strip():  # Only save non-empty chunks
                sess.add(Chunk(paper_id=paper_obj.id, text=txt, chunk_idx=idx))
        sess.commit()

        # Embed + index (use cleaned chunks)
        embeddings = embed_texts(cleaned_chunks)
        docs = []
        for idx, (txt, emb) in enumerate(zip(cleaned_chunks, embeddings)):
            if txt and txt.strip():  # Only index non-empty chunks
                docs.append(
                    {
                        "paper_id": paper_obj.id,
                        "arxiv_id": arxiv_id,
                        "chunk_idx": idx,
                        "text": txt,
                        "embedding": emb,
                    }
                )
        if docs:  # Only index if we have valid documents
            index_chunks_bulk(docs)

    sess.close()

def task_test_search(**kwargs):
    """
    Test multi-field search in OpenSearch.
    """
    results = search_papers(
        query_text="Explainable AI in cancer research",
        category=None,
        year_from=2020,
        year_to=2025,
        size=5
    )
    logger.info(f"Search returned {len(results)} papers")
    for r in results:
        logger.info(f"{r['published_at']} - {r['title']} ({r['arxiv_id']})")

# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------
with DAG(
    dag_id="arxiv_daily_pipeline",
    default_args=default_args,
    schedule_interval="20 23 * * *",  # Run every day at 10:30 PM CST
    catchup=False,
    tags=["arxiv", "ingestion"],
) as dag:

    init = PythonOperator(task_id="init", python_callable=task_init)
    fetch = PythonOperator(task_id="fetch_metadata", python_callable=task_fetch_metadata)
    store_meta = PythonOperator(task_id="store_metadata", python_callable=task_store_metadata)
    download = PythonOperator(task_id="download_pdfs", python_callable=task_download_pdfs)
    process_index = PythonOperator(
        task_id="parse_chunk_embed_index", python_callable=task_parse_chunk_embed_index
    )
    test_search = PythonOperator(
                    task_id="test_search",
                    python_callable=task_test_search
                )

    init >> fetch >> store_meta >> download >> process_index >> test_search
