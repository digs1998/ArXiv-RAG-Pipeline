# ingestion/downloaders/pdfDownloader.py
import asyncio
import aiohttp
from pathlib import Path
import logging
import os

logger = logging.getLogger("ingestion")
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

class PDFDownloader:
    def __init__(self, download_dir=None, max_concurrency=10):
        self.download_dir = Path(download_dir or DATA_DIR) / "pdfs"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        logger.info(f"PDFs will be saved to: {self.download_dir.resolve()}")

    async def _download_pdf(self, session, url, save_path):
        async with self.semaphore:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(save_path, "wb") as f:
                            f.write(await response.read())
                        return save_path
                    else:
                        logger.warning(f"Failed to download {url}: {response.status}")
            except Exception as e:
                logger.error(f"Error downloading {url}: {e}")
        return None

    async def download_batch(self, papers, year_start, year_end):
        tasks = []
        downloaded = []

        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for paper in papers:
                year = paper.get("published_year")
                pdf_url = paper.get("pdf_url")
                logger.info(f"Checking paper {paper['arxiv_id']} ({year}) -> {pdf_url}")

                if year is None:
                    logger.warning(f"Skipping {paper['arxiv_id']} because year is None")
                    continue

                if year_start <= year <= year_end:
                    save_path = self.download_dir / f"{paper['arxiv_id']}.pdf"
                    logger.info(f"Queued for download: {pdf_url} -> {save_path}")
                    tasks.append(self._download_pdf(session, pdf_url, save_path))
                else:
                    logger.info(f"Skipping {paper['arxiv_id']} (year {year} not in range)")

            for future in asyncio.as_completed(tasks):
                result = await future
                if result:
                    downloaded.append(result)
                    logger.info(f"Downloaded {len(downloaded)}/{len(tasks)}: {result.name}")

        return downloaded

