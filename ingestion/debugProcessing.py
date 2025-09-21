import logging
from processing import parse_pdf_with_fallback
from sectionChunker import section_aware_chunking

logger = logging.getLogger("debug")
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # Replace with a real PDF path from your /data/pdfs folder
    pdf_path = "data/pdfs/2004.04742v1.pdf"

    text = parse_pdf_with_fallback(pdf_path)
    if not text:
        logger.info(f"No text parsed for {pdf_path}")
    else:
        logger.info(f"Text parsed. Length: {len(text)} characters")
        
        # If you have a section parser
        try:
            parsed_sections = section_aware_chunking(text, target_words=600, overlap_words=100)
            if not parsed_sections:
                logger.info(f"No parsed sections/chunks for {pdf_path}")
            else:
                logger.info(f"Number of chunks: {len(parsed_sections)}")
                for i, chunk in enumerate(parsed_sections[:5]):  # preview first 5
                    if isinstance(chunk, dict):
                        section = chunk.get("section", "unknown")
                        chunk_text = chunk.get("text", "")
                    elif isinstance(chunk, (tuple, list)):
                        chunk_text = chunk[0]
                        section = chunk[1] if len(chunk) > 1 else "unknown"
                    else:
                        continue
                    logger.info(f"Chunk {i} | Section: {section} | Text preview: {chunk_text[:100]}")
        except Exception as e:
            logger.exception(f"Error during section chunking: {e}")
