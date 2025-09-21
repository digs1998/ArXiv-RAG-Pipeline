# dags/ingestArxiv.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import asyncio
from pathlib import Path

# --- Ingestion imports ---
from ingestion.client.arxivClient import ArxivClient
from ingestion.db import init_db, SessionLocal, upsert_paper, Paper, Chunk
from ingestion.downloaders.pdfDownloader import PDFDownloader
from ingestion.processing import parse_and_chunk_with_sections, clean_text_for_db
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
    "start_date": datetime(2025, 9, 18),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
ARXIV_QUERY = "Explainable AI in cancer research"
YEAR_START = 2020
YEAR_END = 2025
BATCH_SIZE = 20

# -------------------------------------------------------------------
# Task functions
# -------------------------------------------------------------------
def task_init(**kwargs):
    """Initialize database and OpenSearch indices."""
    init_db()
    ensure_indices()

def task_fetch_metadata(**kwargs):
    client = ArxivClient()
    all_papers = []
    start = 0

    while True:
        xml = client.fetch_metadata(query=ARXIV_QUERY, start=start, max_results=BATCH_SIZE)
        papers = client.parse_metadata(xml)
        if not papers:
            break
        all_papers.extend(papers)
        start += BATCH_SIZE
        if start >= 2000:
            break

    print(f"Fetched total {len(all_papers)} papers from arXiv")
    kwargs["ti"].xcom_push(key="papers", value=all_papers)

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
            "published_date": p.get("published_year") and f"{p.get('published_year')}-01-01",
            "pdf_url": p.get("pdf_url"),
        }
        paper_obj = upsert_paper(sess, meta=meta, pdf_path=None)
        saved.append({"arxiv_id": paper_obj.arxiv_id, "id": paper_obj.id, "pdf_url": paper_obj.pdf_url})
        index_paper_meta(meta)

    sess.commit()
    sess.close()
    kwargs["ti"].xcom_push(key="saved_papers", value=saved)

def task_download_pdfs(**kwargs):
    saved = kwargs["ti"].xcom_pull(key="saved_papers", task_ids="store_metadata") or []
    downloader = PDFDownloader()
    sess = SessionLocal()
    papers_for_download = []

    for s in saved:
        paper_obj = sess.query(Paper).filter_by(arxiv_id=s["arxiv_id"]).one_or_none()
        if paper_obj:
            year = (paper_obj.published_date.year
                    if hasattr(paper_obj.published_date, 'year')
                    else int(str(paper_obj.published_date).split('-')[0]))
            papers_for_download.append({
                "arxiv_id": s["arxiv_id"],
                "pdf_url": s["pdf_url"],
                "published_year": year
            })
    sess.close()

    paths = asyncio.run(downloader.download_batch(papers_for_download, YEAR_START, YEAR_END))
    print(f"paths discovered {paths}")
    mapping = {p.name.replace(".pdf", ""): str(p) for p in paths}
    kwargs["ti"].xcom_push(key="pdf_paths", value=mapping)

def task_parse_chunk_embed_index(**kwargs):
    saved = kwargs["ti"].xcom_pull(key="saved_papers", task_ids="store_metadata") or []
    pdf_map = kwargs["ti"].xcom_pull(key="pdf_paths", task_ids="download_pdfs") or {}
    sess = SessionLocal()

    for s in saved:
        arxiv_id = s["arxiv_id"]
        paper_obj = sess.query(Paper).filter_by(arxiv_id=arxiv_id).one_or_none()
        if not paper_obj:
            continue

        pdf_path = pdf_map.get(arxiv_id)
        if not pdf_path:
            logger.warning("No pdf path for %s", arxiv_id)
            continue

        # parse and section-chunk -> returns list of (text, section)
        chunks = parse_and_chunk_with_sections(pdf_path)
        if not chunks:
            logger.info("No chunks for %s", arxiv_id)
            continue

        # Save chunk records to DB
        for idx, (txt, section) in enumerate(chunks):
            sess.add(Chunk(paper_id=paper_obj.id, text=txt, chunk_idx=idx, section=section))
        sess.commit()

        # Generate embeddings (batch)
        texts = [txt for txt, _ in chunks]
        embeddings = embed_texts(texts)

        # Build docs and bulk index
        docs = []
        for idx, ((txt, section), emb) in enumerate(zip(chunks, embeddings)):
            docs.append({
                "paper_id": paper_obj.id,
                "arxiv_id": arxiv_id,
                "chunk_idx": idx,
                "text": txt,
                "section": section,
                "embedding": emb
            })
        if docs:
            index_chunks_bulk(docs)

    sess.close()

def task_test_search(**kwargs):
    results = search_papers(query_text=ARXIV_QUERY, size=5)
    logger.info(f"Search returned {len(results)} papers")
    for r in results:
        logger.info(f"{r.get('published_at')} - {r.get('title')} ({r.get('arxiv_id')})")

# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------
with DAG(
    dag_id="arxiv_daily_pipeline",
    default_args=default_args,
    schedule_interval="20 23 * * *",
    catchup=False,
    tags=["arxiv", "ingestion"],
) as dag:

    init = PythonOperator(task_id="init", python_callable=task_init)
    fetch = PythonOperator(task_id="fetch_metadata", python_callable=task_fetch_metadata)
    store_meta = PythonOperator(task_id="store_metadata", python_callable=task_store_metadata)
    download = PythonOperator(task_id="download_pdfs", python_callable=task_download_pdfs)
    process_index = PythonOperator(task_id="parse_chunk_embed_index", python_callable=task_parse_chunk_embed_index)
    test_search = PythonOperator(task_id="test_search", python_callable=task_test_search)

    init >> fetch >> store_meta >> download >> process_index >> test_search
