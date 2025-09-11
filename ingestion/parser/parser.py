import pdfplumber
import requests
import logging
import os
from pathlib import Path

logger = logging.getLogger("ingestion")


class GrobidParser:
    def __init__(self, grobid_url=None):
        self.grobid_url = grobid_url or os.getenv("GROBID_URL")

    def parse(self, pdf_path: str) -> str:
        try:
            with open(pdf_path, "rb") as f:
                files = {"input": f}
                response = requests.post(self.grobid_url, files=files)
                response.raise_for_status()
                return response.text
        except Exception as e:
            logger.warning(f"GROBID parsing failed for {pdf_path}: {e}")
            return None


class DoclingParser:
    def parse(self, pdf_path: str) -> str:
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            logger.error(f"Docling fallback failed for {pdf_path}: {e}")
        return text or None


class PDFParser:
    """
    Unified parser: tries GROBID first, then Docling fallback.
    """
    def __init__(self, grobid_url=None):
        self.parsers = [
            GrobidParser(grobid_url=grobid_url),
            DoclingParser()
        ]

    def parse(self, pdf_path: str) -> str:
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            logger.warning(f"{pdf_path} does not exist")
            return ""

        text = None
        for parser in self.parsers:
            try:
                text = parser.parse(str(pdf_path))
                if text:
                    break
            except Exception as e:
                logger.warning(f"{parser.__class__.__name__} failed for {pdf_path}: {e}")

        if not text:
            logger.warning(f"No parser succeeded for {pdf_path}")
            return ""

        return text

    @staticmethod
    def chunk_text(text: str, chunk_size=1000, overlap=100):
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start += chunk_size - overlap
        return chunks
