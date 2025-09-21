# ingestion/processing.py
from pathlib import Path
import logging
from typing import List, Tuple, Optional

from ingestion.parser.parser import PDFParser
from ingestion.pdfUtils import extract_text_from_pdf, parse_tei_xml
from ingestion.sectionChunker import section_aware_chunking, merge_small_chunks

logger = logging.getLogger("ingestion")

# PDF parser wrapper (reads GROBID_URL from env inside PDFParser)
parser = PDFParser(grobid_url=None)


def clean_text_for_db(text: str) -> str:
    """Normalize text before saving to DB or indexing."""
    if not text:
        return ""
    return text.replace("\x00", " ").strip()


def parse_pdf_with_fallback(pdf_path: str) -> str:
    """
    Parse a PDF and return all text as a single string.
    Tries GROBID first, falls back to raw PDF extraction.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        logger.warning("parse_pdf_with_fallback: %s does not exist", pdf_path)
        return ""

    # Try GROBID (TEI)
    try:
        xml_text = parser.parse(str(pdf_path))
        if xml_text and xml_text.strip().startswith("<?xml"):
            text = parse_tei_xml(xml_text)
            if text and text.strip():
                return text
            logger.warning("GROBID returned empty text for %s", pdf_path)
    except Exception as e:
        logger.warning("GROBID parsing failed for %s: %s", pdf_path, e)

    # Fallback to raw PDF extraction
    try:
        pages = extract_text_from_pdf(str(pdf_path))
        if pages:
            text = " ".join([p[0] for p in pages if p[0].strip()])
            return text
        else:
            logger.warning("extract_text_from_pdf produced no pages for %s", pdf_path)
    except Exception as e:
        logger.error("Docling/raw PDF fallback failed for %s: %s", pdf_path, e)

    return ""


def parse_and_chunk_with_sections(
    pdf_path: str,
    min_tokens: int = 50
) -> List[Tuple[str, Optional[str]]]:
    """
    Parse PDF (GROBID -> fallback) and produce section-aware chunks.

    Returns:
        List of tuples: (chunk_text, section_name)
    """
    text = parse_pdf_with_fallback(pdf_path)
    if not text or not text.strip():
        return []

    # Step 1: create raw chunks from full text
    raw_chunks = section_aware_chunking(text, target_words=600, overlap_words=100)
    if not raw_chunks:
        return []

    # Step 2: merge small chunks
    merged_chunks = merge_small_chunks(raw_chunks, min_tokens=min_tokens)

    # Step 3: normalize and prepare output
    out = []
    for c in merged_chunks:
        if isinstance(c, dict):
            chunk_text = c.get("text", "")
            section = c.get("section", "unknown")
        elif isinstance(c, (list, tuple)):
            chunk_text = c[0]
            section = c[1] if len(c) > 1 else "unknown"
        else:
            continue

        if chunk_text and chunk_text.strip():
            out.append((clean_text_for_db(chunk_text), section))

    return out
