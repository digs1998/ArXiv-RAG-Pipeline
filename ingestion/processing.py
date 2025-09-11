# ingestion/processing.py
from ingestion.parser.parser import PDFParser
from pathlib import Path
import logging

logger = logging.getLogger("ingestion")
parser = PDFParser(grobid_url=None)  # it will read GROBID_URL from env

def parse_and_chunk(pdf_path, chunk_size=1000, overlap=100):
    text = parser.parse(pdf_path)
    if not text:
        return []
    # use parser.chunk_text which you implemented earlier
    chunks = parser.chunk_text(text, chunk_size=chunk_size, overlap=overlap)
    return chunks
