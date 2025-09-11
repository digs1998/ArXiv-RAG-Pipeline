import requests
import os
import logging
from typing import List

logger = logging.getLogger("ingestion")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
# FIXED: Use the full model name with :latest tag
OLLAMA_MODEL = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text:latest")

def embed_texts_ollama(texts: List[str]) -> List[List[float]]:
    """Get embeddings from Ollama for a list of texts."""
    if not texts:
        return []
    
    # Try batch processing first (much faster)
    batch_size = 10  # Process 10 texts at once
    all_embeddings = []
    
    for batch_start in range(0, len(texts), batch_size):
        batch_end = min(batch_start + batch_size, len(texts))
        batch_texts = texts[batch_start:batch_end]
        
        # Filter out empty texts in this batch
        valid_batch_texts = []
        empty_indices = []
        
        for i, text in enumerate(batch_texts):
            if not text or not text.strip():
                empty_indices.append(batch_start + i)
                valid_batch_texts.append("empty")  # placeholder
            else:
                valid_batch_texts.append(text)
        
        if not valid_batch_texts:
            # All texts in batch are empty
            all_embeddings.extend([[0.0] * 768] * len(batch_texts))
            continue
            
        try:
            # Try batch API call first
            payload = {
                "model": OLLAMA_MODEL,
                "input": valid_batch_texts
            }
            
            resp = requests.post(f"{OLLAMA_URL}/api/embed", json=payload, timeout=60)
            
            if resp.status_code == 200:
                result = resp.json()
                
                if "embeddings" in result and result["embeddings"]:
                    batch_embeddings = result["embeddings"]
                    
                    # Replace empty text embeddings with zeros
                    for i, global_idx in enumerate(range(batch_start, batch_end)):
                        if global_idx in empty_indices:
                            batch_embeddings[i] = [0.0] * 768
                    
                    all_embeddings.extend(batch_embeddings)
                    logger.info(f"Batch processed {batch_end}/{len(texts)} embeddings")
                    continue
                    
        except Exception as e:
            logger.warning(f"Batch processing failed for batch {batch_start}-{batch_end}: {e}")
        
        # Fallback to individual processing for this batch
        logger.info(f"Falling back to individual processing for batch {batch_start}-{batch_end}")
        for i, text in enumerate(batch_texts):
            global_idx = batch_start + i
            
            if not text or not text.strip():
                all_embeddings.append([0.0] * 768)
                continue
                
            payload = {
                "model": OLLAMA_MODEL,
                "input": text
            }
            
            try:
                resp = requests.post(f"{OLLAMA_URL}/api/embed", json=payload, timeout=30)
                
                if resp.status_code != 200:
                    logger.error(f"Ollama embedding failed for text {global_idx}: {resp.status_code} - {resp.text}")
                    resp.raise_for_status()
                
                result = resp.json()
                
                # Handle response format
                embedding = None
                if "embeddings" in result and result["embeddings"]:
                    embedding = result["embeddings"][0]
                elif "embedding" in result:
                    embedding = result["embedding"]
                else:
                    raise Exception(f"Invalid embedding response format: {result}")
                    
                if not embedding or not isinstance(embedding, list):
                    raise Exception("Invalid embedding format")
                    
                all_embeddings.append(embedding)
                
                if (global_idx + 1) % 10 == 0:
                    logger.info(f"Individual processing: {global_idx + 1}/{len(texts)} embeddings")
                    
            except Exception as e:
                logger.error(f"Error embedding text {global_idx}: {e}")
                raise
            
    logger.info(f"Successfully generated {len(all_embeddings)} embeddings using {OLLAMA_MODEL}")
    return all_embeddings


def embed_texts(texts: List[str]) -> List[List[float]]:
    """Main function to get embeddings using Ollama."""
    if not texts:
        logger.warning("No texts provided for embedding")
        return []
    
    # Filter out empty texts and log warning
    valid_texts = []
    for i, text in enumerate(texts):
        if text and text.strip():
            valid_texts.append(text)
        else:
            logger.warning(f"Skipping empty text at index {i}")
    
    if not valid_texts:
        logger.warning("No valid texts found after filtering")
        return []
    
    logger.info(f"Embedding {len(valid_texts)} texts using Ollama ({OLLAMA_MODEL})")
    
    return embed_texts_ollama(valid_texts)


# Test function for debugging
def test_embeddings():
    """Test function to verify embeddings work."""
    test_texts = ["Hello world", "This is a test"]
    try:
        embeddings = embed_texts(test_texts)
        print(f"Success! Generated {len(embeddings)} embeddings")
        print(f"First embedding dimension: {len(embeddings[0])}")
        return True
    except Exception as e:
        print(f"Test failed: {e}")
        return False