# ingestion/pdf_utils.py
import fitz  # PyMuPDF
from lxml import etree
import logging
from typing import List, Tuple

logger = logging.getLogger("ingestion")


def extract_text_from_pdf(pdf_path: str) -> List[Tuple[str, int]]:
    """
    Extract all text from a PDF using PyMuPDF.
    Returns a list of (page_text, page_number) tuples.
    """
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        logger.error("Failed to open PDF %s: %s", pdf_path, e)
        return []

    pages_text = []
    for page_number, page in enumerate(doc, start=1):
        try:
            text = page.get_text("text")
            if text and text.strip():
                pages_text.append((text, page_number))
        except Exception as e:
            logger.warning("Failed to extract text from page %s of %s: %s", page_number, pdf_path, e)
    try:
        doc.close()
    except Exception:
        pass
    return pages_text


def parse_tei_xml(xml_text: str) -> str:
    """
    Extract readable text from GROBID TEI XML output.
    Joins section headers (<head>) and paragraphs (<p>).
    """
    try:
        parser_lxml = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_text.encode("utf-8"), parser=parser_lxml)
    except Exception as e:
        logger.error("Failed to parse TEI XML: %s", e)
        return ""

    ns = {"tei": "http://www.tei-c.org/ns/1.0"}
    texts = []

    # Iterate divs under body (typical structure for GROBID TEI)
    for elem in root.xpath("//tei:body//tei:div", namespaces=ns):
        head = elem.find(".//tei:head", namespaces=ns)
        if head is not None and head.text:
            texts.append(head.text.strip())
        for p in elem.findall(".//tei:p", namespaces=ns):
            # paragraph text may contain nested tags; using .text is ok for simple extraction
            paragraph_text = "".join(p.itertext()).strip()
            if paragraph_text:
                texts.append(paragraph_text)

    # As fallback, attempt to extract <p> elements anywhere
    if not texts:
        for p in root.xpath("//tei:p", namespaces=ns):
            paragraph_text = "".join(p.itertext()).strip()
            if paragraph_text:
                texts.append(paragraph_text)

    return "\n".join(texts)
