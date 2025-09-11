import logging
import time
from ingestion.client.arxivClient import ArxivClient
from ingestion.downloaders.pdfDownloader import PDFDownloader
from ingestion.parser.pdf_parser import PDFParser
import xml.etree.ElementTree as ET
from pathlib import Path
import os

logger = logging.getLogger("ingestion")
logging.basicConfig(level=logging.INFO)

grobid_url = os.getenv("GROBID_URL")

class IngestionPipeline:
    def __init__(self, query: str, grobid_url=None):
        self.query = query
        self.client = ArxivClient()
        self.downloader = PDFDownloader()
        self.parser = PDFParser(grobid_url=grobid_url)
        self.all_papers = []

    def get_total_papers_count(self, year_start: int, year_end: int) -> int:
        query = f"{self.query} AND submittedDate:[{year_start}0101 TO {year_end}1231]"
        xml = self.client.fetch_metadata(query=query, start=0, max_results=1)
        root = ET.fromstring(xml)
        ns = {"opensearch": "http://a9.com/-/spec/opensearch/1.1/"}
        total_results_elem = root.find("opensearch:totalResults", ns)
        if total_results_elem is None:
            raise ValueError("Cannot find totalResults in ArXiv API response")
        return int(total_results_elem.text)

    def run(self, year_start: int, year_end: int, batch_size: int = 50, max_results: int = None):
        start = 0
        self.all_papers = []
        query = f"{self.query} AND submittedDate:[{year_start}0101 TO {year_end}1231]"

        if max_results is None:
            max_results = self.get_total_papers_count(year_start, year_end)

        while start < max_results:
            xml = self.client.fetch_metadata(query=query, start=start, max_results=batch_size)
            papers = self.client.parse_metadata(xml)
            if not papers:
                break
            self.all_papers.extend(papers)
            logger.info(f"Fetched {len(self.all_papers)} papers so far")
            start += batch_size
            time.sleep(3)  # Respect rate limit

        logger.info(f"Total fetched papers: {len(self.all_papers)}")

        downloaded_paths = self.downloader.download_batch(self.all_papers, year_start, year_end)
        logger.info(f"Downloaded {len(downloaded_paths)} PDFs")

        for paper in self.all_papers:
            pdf_path = Path(self.downloader.save_dir) / f"{paper['arxiv_id']}.pdf"
            if pdf_path.exists():
                text = self.parser.parse(str(pdf_path))
                chunks = self.parser.chunk_text(text)
                paper["chunks"] = chunks
                logger.info(f"{paper['arxiv_id']}: {len(chunks)} chunks")
            else:
                logger.warning(f"{pdf_path} not found, skipping parsing")

        return self.all_papers


# if __name__ == "__main__":
#     import os
#     grobid_url = os.getenv("GROBID_URL")  # use Docker network or env var
#     pipeline = IngestionPipeline(query="Explainable AI in cancer research", grobid_url=grobid_url)

#     total_papers = pipeline.get_total_papers_count(year_start=2020, year_end=2025)
#     logger.info(f"Total papers to fetch: {total_papers}")

#     papers_with_chunks = pipeline.run(year_start=2020, year_end=2025, batch_size=50, max_results=total_papers)
