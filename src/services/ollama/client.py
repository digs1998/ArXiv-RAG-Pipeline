import json
import logging
from typing import Any, Dict, List, Optional
import aiohttp

import httpx
from src.config import Settings
from src.exceptions import OllamaConnectionError, OllamaException, OllamaTimeoutError
from src.schemas.ollama import RAGResponse
from src.services.ollama.prompts import RAGPromptBuilder, ResponseParser

logger = logging.getLogger(__name__)


class OllamaClient:
    """Client for interacting with Ollama local LLM service."""
    
    def __init__(self, host: str, model: str, timeout: int, session: Optional[aiohttp.ClientSession] = None):
        self.base_url = host.rstrip("/")
        self.host = self.base_url   # alias for backward compatibility
        self.model = model
        self.session = session or aiohttp.ClientSession()
        self.timeout = httpx.Timeout(float(timeout))
        self.prompt_builder = RAGPromptBuilder()
        self.response_parser = ResponseParser()

        
    async def health_check(self) -> Dict[str, Any]:
        """
        Check if Ollama service is healthy and responding.

        Returns:
            Dictionary with health status information
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Check version endpoint for health
                response = await client.get(f"{self.base_url}/api/version")

                if response.status_code == 200:
                    version_data = response.json()
                    return {
                        "status": "healthy",
                        "message": "Ollama service is running",
                        "version": version_data.get("version", "unknown"),
                    }
                else:
                    raise OllamaException(f"Ollama returned status {response.status_code}")

        except httpx.ConnectError as e:
            raise OllamaConnectionError(f"Cannot connect to Ollama service: {e}")
        except httpx.TimeoutException as e:
            raise OllamaTimeoutError(f"Ollama service timeout: {e}")
        except OllamaException:
            raise
        except Exception as e:
            raise OllamaException(f"Ollama health check failed: {str(e)}")

    async def list_models(self) -> List[Dict[str, Any]]:
        """
        Get list of available models.

        Returns:
            List of model information dictionaries
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.base_url}/api/tags")

                if response.status_code == 200:
                    data = response.json()
                    return data.get("models", [])
                else:
                    raise OllamaException(f"Failed to list models: {response.status_code}")

        except httpx.ConnectError as e:
            raise OllamaConnectionError(f"Cannot connect to Ollama service: {e}")
        except httpx.TimeoutException as e:
            raise OllamaTimeoutError(f"Ollama service timeout: {e}")
        except OllamaException:
            raise
        except Exception as e:
            raise OllamaException(f"Error listing models: {e}")

    async def generate(self, model: str, prompt: str, stream: bool = False, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Generate text using specified model.

        Args:
            model: Model name to use
            prompt: Input prompt for generation
            stream: Whether to stream response
            **kwargs: Additional generation parameters

        Returns:
            Response dictionary or None if failed
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                data = {"model": model, "prompt": prompt, "stream": stream, **kwargs}

                logger.info(f"Sending request to Ollama: model={model}, stream={stream}, extra_params={kwargs}")
                response = await client.post(f"{self.base_url}/api/chat", json=data)

                if response.status_code == 200:
                    return response.json()
                else:
                    raise OllamaException(f"Generation failed: {response.status_code}")

        except httpx.ConnectError as e:
            raise OllamaConnectionError(f"Cannot connect to Ollama service: {e}")
        except httpx.TimeoutException as e:
            raise OllamaTimeoutError(f"Ollama service timeout: {e}")
        except OllamaException:
            raise
        except Exception as e:
            raise OllamaException(f"Error generating with Ollama: {e}")

    # async def generate_stream(self, model: str, prompt: str, **kwargs):
    #     """
    #     Generate text with streaming response.

    #     Args:
    #         model: Model name to use
    #         prompt: Input prompt for generation
    #         **kwargs: Additional generation parameters

    #     Yields:
    #         JSON chunks from streaming response
    #     """
    #     try:
    #         async with httpx.AsyncClient(timeout=self.timeout) as client:
    #             data = {"model": model, "prompt": prompt, "stream": True, **kwargs}

    #             logger.info(f"Starting streaming generation: model={model}")

    #             async with client.stream("POST", f"{self.base_url}/api/chat", json=data) as response:
    #                 if response.status_code != 200:
    #                     raise OllamaException(f"Streaming generation failed: {response.status_code}")

    #                 async for line in response.aiter_lines():
    #                     if line.strip():
    #                         try:
    #                             chunk = json.loads(line)
    #                             yield chunk
    #                         except json.JSONDecodeError:
    #                             logger.warning(f"Failed to parse streaming chunk: {line}")
    #                             continue

    #     except httpx.ConnectError as e:
    #         raise OllamaConnectionError(f"Cannot connect to Ollama service: {e}")
    #     except httpx.TimeoutException as e:
    #         raise OllamaTimeoutError(f"Ollama service timeout: {e}")
    #     except OllamaException:
    #         raise
    #     except Exception as e:
    #         raise OllamaException(f"Error in streaming generation: {e}")
    
    async def generate_stream(self, model: str, prompt: str, **kwargs):
        url = f"{self.host}/api/generate"
        payload = {
            "model": model,
            "prompt": prompt,
            **kwargs,
            "stream": True
        }

        async with self.session.post(url, json=payload) as resp:
            async for line in resp.content:
                if not line:
                    continue
                try:
                    data = json.loads(line.decode("utf-8").strip())
                    yield data   # ✅ now it’s a dict, not a str
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode line: {line}")



    async def generate_rag_answer(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        model: str = "llama3.2",
        use_structured_output: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate a RAG answer using retrieved chunks.

        Args:
            query: User's question
            chunks: Retrieved document chunks with metadata
            model: Model to use for generation
            use_structured_output: Whether to use Ollama's structured output feature

        Returns:
            Dictionary with answer, sources, confidence, and citations
        """
        try:
            if use_structured_output:
                # Use structured output with Pydantic model
                prompt_data = self.prompt_builder.create_structured_prompt(query, chunks)

                # Generate with structured format
                response = await self.generate(
                    model=model,
                    prompt=prompt_data["prompt"],
                    temperature=0.7,
                    top_p=0.9,
                    format=prompt_data["format"],
                )
            else:
                # Fallback to JSON mode
                prompt = self.prompt_builder.create_rag_prompt(query, chunks)

                # Generate with JSON format instruction
                response = await self.generate(
                    model=model,
                    prompt=prompt,
                    temperature=0.7,
                    top_p=0.9,
                    format="json",
                )

            if response and "response" in response:
                # Parse the LLM response
                logger.debug(f"Raw LLM response: {response['response'][:500]}")
                parsed_response = self.response_parser.parse_structured_response(response["response"])
                logger.debug(f"Parsed response: {parsed_response}")

                # Ensure sources are included if not already
                if not parsed_response.get("sources"):
                    # Build PDF URLs from arxiv_ids
                    sources = []
                    seen_urls = set()
                    for chunk in chunks:
                        arxiv_id = chunk.get("arxiv_id")
                        if arxiv_id:
                            # Build PDF URL from arxiv_id
                            arxiv_id_clean = arxiv_id.split("v")[0] if "v" in arxiv_id else arxiv_id
                            pdf_url = f"https://arxiv.org/pdf/{arxiv_id_clean}.pdf"
                            if pdf_url not in seen_urls:
                                sources.append(pdf_url)
                                seen_urls.add(pdf_url)
                    parsed_response["sources"] = sources

                # Add citations if not present
                if not parsed_response.get("citations"):
                    # Extract unique arxiv IDs
                    citations = list(set(chunk.get("arxiv_id") for chunk in chunks if chunk.get("arxiv_id")))
                    parsed_response["citations"] = citations[:5]  # Limit to 5 citations

                return parsed_response
            else:
                raise OllamaException("No response generated from Ollama")

        except Exception as e:
            logger.error(f"Error generating RAG answer: {e}")
            raise OllamaException(f"Failed to generate RAG answer: {e}")

    async def generate_rag_answer_stream(
        self,
        query: str,
        chunks: List[Dict[str, Any]],
        model: str = "llama3.2",
    ):
        """
        Generate a streaming RAG answer using retrieved chunks.

        Args:
            query: User's question
            chunks: Retrieved document chunks with metadata
            model: Model to use for generation

        Yields:
            Streaming response chunks with partial answers
        """
        try:
            # Create prompt for streaming (simpler than structured)
            prompt = self.prompt_builder.create_rag_prompt(query, chunks)

            # Stream the response
            async for chunk in self.generate_stream(
                model=model,
                prompt=prompt,
                temperature=0.7,
                top_p=0.9,
            ):
                yield chunk

        except Exception as e:
            logger.error(f"Error generating streaming RAG answer: {e}")
            raise OllamaException(f"Failed to generate streaming RAG answer: {e}")