# ingestion/sectionChunker.py
import re
from typing import List, Dict


def section_aware_chunking(
    text: str,
    target_words: int = 600,
    overlap_words: int = 100
) -> List[Dict[str, str]]:
    """
    Split text into section-aware chunks with overlap.
    Dynamically detects sections instead of hardcoding.
    
    Args:
        text: The full text of the paper.
        target_words: Desired chunk size (default ~600 words).
        overlap_words: Overlap between consecutive chunks (default ~100 words).
    
    Returns:
        List of dicts with {"text": ..., "section": ...}.
    """
    # --- Step 1: Split into sections dynamically ---
    # simple heuristic: a line with ALL CAPS or numbered headings = section
    section_splits = re.split(r"\n(?=[A-Z][A-Z\s0-9]{2,}\n)", text)
    
    chunks = []

    for section_text in section_splits:
        lines = section_text.strip().split("\n")
        if not lines:
            continue

        # First line = section name if looks like a heading
        section_name = lines[0].strip()
        body = " ".join(lines[1:]).strip()

        if not body:
            continue

        words = body.split()
        start = 0

        # --- Step 2: Chunk with overlap ---
        while start < len(words):
            end = min(start + target_words, len(words))
            chunk_text = " ".join(words[start:end]).strip()

            chunks.append({
                "text": chunk_text,
                "section": section_name
            })

            start += target_words - overlap_words

    return chunks


def merge_small_chunks(chunks, min_tokens=50):
    merged, buffer, current_section = [], [], None
    for chunk in chunks:
        tokens = len(chunk["text"].split())
        section = chunk["section"]
        if tokens < min_tokens or section.lower() == "unknown":
            buffer.append(chunk["text"])
            current_section = current_section or section
        else:
            if buffer:
                merged.append({"section": current_section or "unknown", "text": " ".join(buffer)})
                buffer = []
            merged.append(chunk)
            current_section = section
    if buffer:
        merged.append({"section": current_section or "unknown", "text": " ".join(buffer)})
    return merged
