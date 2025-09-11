# ingestion/clients/arxiv_client.py
import logging
from typing import List, Dict, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET

logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class ArxivClient:
    """
    Handles fetching and parsing metadata from arXiv API.
    """

    BASE_URL = "https://export.arxiv.org/api/query"

    def __init__(self, max_retries: int = 3, timeout: int = 30):
        self.max_retries = max_retries
        self.timeout = timeout
        self.client = httpx.Client(timeout=self.timeout)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch_metadata(self, query: str, start: int = 0, max_results: int = 50) -> str:
        """
        Fetch metadata from arXiv API with retry mechanism.
        Returns raw XML content.
        """
        params = {"search_query": query, "start": start, "max_results": max_results}
        logger.info(f"Fetching metadata from arXiv: {params}")

        response = self.client.get(self.BASE_URL, params=params, follow_redirects=True)
        response.raise_for_status()
        return response.text

    @staticmethod
    def parse_metadata(xml_content: str) -> List[Dict[str, str]]:
        """
        Parse arXiv API XML feed.
        Returns a list of dictionaries with keys:
        arxiv_id, title, pdf_url, published_year
        """
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        root = ET.fromstring(xml_content)
        papers: List[Dict[str, str]] = []

        for entry in root.findall("atom:entry", ns):
            paper_id = entry.find("atom:id", ns).text.split("/")[-1]
            title = entry.find("atom:title", ns).text.strip()
            published = entry.find("atom:published", ns).text
            published_year = int(published[:4])

            # Default PDF URL
            pdf_url = f"https://arxiv.org/pdf/{paper_id}.pdf"
            # Check for official PDF link if available
            for link in entry.findall("atom:link", ns):
                if link.attrib.get("title") == "pdf":
                    pdf_url = link.attrib.get("href", pdf_url)

            papers.append({
                "arxiv_id": paper_id,
                "title": title,
                "pdf_url": pdf_url,
                "published_year": published_year
            })

        return papers

    def fetch_and_parse(self, query: str, start: int = 0, max_results: int = 50) -> List[Dict[str, str]]:
        """
        Convenience method to fetch and parse in one step.
        """
        xml_content = self.fetch_metadata(query=query, start=start, max_results=max_results)
        papers = self.parse_metadata(xml_content)
        logger.info(f"Parsed {len(papers)} papers from arXiv")
        return papers

    def close(self):
        self.client.close()
