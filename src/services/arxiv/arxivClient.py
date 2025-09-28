# arxiv_client_enhanced.py
import asyncio
import logging
import time
from functools import cached_property
from pathlib import Path
from typing import List, Optional

import httpx
import backoff  # optional: if not available, I include a basic retry fallback below

from src.config import ArxivSettings
from src.exceptions import (
    ArxivAPIException,
    ArxivAPITimeoutError,
    ArxivParseError,
    PDFDownloadException,
    PDFDownloadTimeoutError,
)
from src.schemas.arxiv.paper import ArxivPaper
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

# --- BACKOFF: simple fallback if you don't want to add 'backoff' dependency ---
def _backoff_hdlr(details):
    logger.warning(
        "Retrying download: attempt %(tries)s after exception %(value)s",
        {"tries": details.get("tries"), "value": details.get("value")},
    )


def _is_pdf_bytes(data: bytes) -> bool:
    return data[:5].startswith(b"%PDF-")


class ArxivClient:
    """Client for fetching papers + robust PDF downloads."""

    def __init__(self, settings: ArxivSettings):
        self._settings = settings
        self._last_request_time: Optional[float] = None
        self._download_semaphore = asyncio.Semaphore(self._settings.download_max_concurrency or 4)

    # ---------- Convenience / settings ----------
    @cached_property
    def pdf_cache_dir(self) -> Path:
        cache_dir = Path(self._settings.pdf_cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    @property
    def base_url(self) -> str:
        return self._settings.base_url

    @property
    def namespaces(self) -> dict:
        return self._settings.namespaces

    @property
    def rate_limit_delay(self) -> float:
        # recommended: arXiv asks ~3s between API calls per client
        return self._settings.rate_limit_delay or 3.0

    @property
    def timeout_seconds(self) -> float:
        return float(self._settings.timeout_seconds or 30)

    @property
    def max_results(self) -> int:
        return int(self._settings.max_results or 100)

    # ---------- Metadata fetching (unchanged behavior, with enforced rate-limiting) ----------
    async def _throttle(self):
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.rate_limit_delay:
                sleep_for = self.rate_limit_delay - elapsed
                logger.debug(f"Throttling: sleeping {sleep_for:.2f}s to respect arXiv rate limits")
                await asyncio.sleep(sleep_for)
        self._last_request_time = time.time()

    async def fetch_papers(
        self,
        max_results: Optional[int] = None,
        start: int = 0,
        sort_by: str = "submittedDate",
        sort_order: str = "descending",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[ArxivPaper]:
        if max_results is None:
            max_results = self.max_results

        
        search_query = f"cat:{self._settings.search_category}"
        
        if len(self._settings.search_category) == 1:
            search_query = f"cat:{self._settings.search_category[0]}"
        else:
            joined_cats = " OR ".join([f"cat:{cat}" for cat in self._settings.search_category])
            search_query = f"({joined_cats})"

        if from_date or to_date:
            date_from = f"{from_date}2000" if from_date else "*"
            date_to = f"{to_date}2359" if to_date else "*"
            search_query += f" AND submittedDate:[{date_from}+TO+{date_to}]"

        params = {
            "search_query": search_query,
            "start": start,
            "max_results": min(max_results, 2000),
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }

        # Build URL preserving arXiv special chars
        safe = ":+[]"
        from urllib.parse import urlencode, quote
        url = f"{self.base_url}?{urlencode(params, quote_via=quote, safe=safe)}"

        try:
            await self._throttle()
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.get(url, headers={"User-Agent": "arXivIngestBot/1.0"})
                resp.raise_for_status()
                xml_data = resp.text

            papers = self._parse_response(xml_data)
            logger.info(f"Fetched {len(papers)} papers")
            return papers

        except httpx.TimeoutException as e:
            logger.error(f"arXiv API timeout: {e}")
            raise ArxivAPITimeoutError(str(e))
        except httpx.HTTPStatusError as e:
            logger.error(f"arXiv API HTTP error: {e}")
            raise ArxivAPIException(f"arXiv API returned HTTP error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching papers: {e}")
            raise ArxivAPIException(str(e))

    async def fetch_papers_with_query(
        self,
        search_query: str,
        max_results: Optional[int] = None,
        start: int = 0,
        sort_by: str = "submittedDate",
        sort_order: str = "descending",
    ) -> List[ArxivPaper]:
        if max_results is None:
            max_results = self.max_results

        params = {
            "search_query": search_query,
            "start": start,
            "max_results": min(max_results, 2000),
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }

        safe = ":+[]*"
        from urllib.parse import urlencode, quote
        url = f"{self.base_url}?{urlencode(params, quote_via=quote, safe=safe)}"

        try:
            await self._throttle()
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.get(url, headers={"User-Agent": "arXivIngestBot/1.0"})
                resp.raise_for_status()
                xml_data = resp.text

            papers = self._parse_response(xml_data)
            logger.info(f"Query returned {len(papers)} papers")
            return papers

        except httpx.TimeoutException as e:
            logger.error(f"arXiv API timeout: {e}")
            raise ArxivAPITimeoutError(str(e))
        except httpx.HTTPStatusError as e:
            logger.error(f"arXiv API HTTP error: {e}")
            raise ArxivAPIException(f"arXiv API returned HTTP error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching papers: {e}")
            raise ArxivAPIException(str(e))

    async def fetch_paper_by_id(self, arxiv_id: str) -> Optional[ArxivPaper]:
        clean_id = arxiv_id.split("v")[0] if "v" in arxiv_id else arxiv_id
        params = {"id_list": clean_id, "max_results": 1}
        safe = ":+[]*"
        from urllib.parse import urlencode, quote
        url = f"{self.base_url}?{urlencode(params, quote_via=quote, safe=safe)}"

        try:
            await self._throttle()
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.get(url, headers={"User-Agent": "arXivIngestBot/1.0"})
                resp.raise_for_status()
                xml_data = resp.text

            papers = self._parse_response(xml_data)
            return papers[0] if papers else None

        except httpx.TimeoutException as e:
            logger.error(f"arXiv API timeout for paper {arxiv_id}: {e}")
            raise ArxivAPITimeoutError(str(e))
        except httpx.HTTPStatusError as e:
            logger.error(f"arXiv API HTTP error for {arxiv_id}: {e}")
            raise ArxivAPIException(f"arXiv API returned HTTP error: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch paper {arxiv_id}: {e}")
            raise ArxivAPIException(str(e))

    # ---------- XML parsing ----------
    def _parse_response(self, xml_data: str) -> List[ArxivPaper]:
        try:
            root = ET.fromstring(xml_data)
            entries = root.findall("atom:entry", self.namespaces)
            papers = []
            for entry in entries:
                p = self._parse_single_entry(entry)
                if p:
                    papers.append(p)
            return papers
        except ET.ParseError as e:
            logger.error(f"Failed to parse arXiv XML response: {e}")
            raise ArxivParseError(str(e))

    def _parse_single_entry(self, entry: ET.Element) -> Optional[ArxivPaper]:
        try:
            arxiv_id = self._get_arxiv_id(entry)
            if not arxiv_id:
                return None
            title = self._get_text(entry, "atom:title", clean_newlines=True)
            authors = self._get_authors(entry)
            abstract = self._get_text(entry, "atom:summary", clean_newlines=True)
            published = self._get_text(entry, "atom:published")
            categories = self._get_categories(entry)
            pdf_url = self._get_pdf_url(entry)
            return ArxivPaper(
                arxiv_id=arxiv_id,
                title=title,
                authors=authors,
                abstract=abstract,
                published_date=published,
                categories=categories,
                pdf_url=pdf_url,
            )
        except Exception as e:
            logger.exception("Failed to parse single entry")
            return None

    def _get_text(self, element: ET.Element, path: str, clean_newlines: bool = False) -> str:
        elem = element.find(path, self.namespaces)
        if elem is None or elem.text is None:
            return ""
        text = elem.text.strip()
        return text.replace("\n", " ") if clean_newlines else text

    def _get_arxiv_id(self, entry: ET.Element) -> Optional[str]:
        id_elem = entry.find("atom:id", self.namespaces)
        if id_elem is None or id_elem.text is None:
            return None
        return id_elem.text.split("/")[-1]

    def _get_authors(self, entry: ET.Element) -> List[str]:
        authors = []
        for author in entry.findall("atom:author", self.namespaces):
            name = self._get_text(author, "atom:name")
            if name:
                authors.append(name)
        return authors

    def _get_categories(self, entry: ET.Element) -> List[str]:
        categories = []
        for category in entry.findall("atom:category", self.namespaces):
            term = category.get("term")
            if term:
                categories.append(term)
        return categories

    def _get_pdf_url(self, entry: ET.Element) -> str:
        for link in entry.findall("atom:link", self.namespaces):
            if link.get("type") == "application/pdf":
                url = link.get("href", "")
                if url.startswith("http://arxiv.org/"):
                    url = url.replace("http://arxiv.org/", "https://arxiv.org/")
                return url
        # fallback construct
        arxiv_id = self._get_arxiv_id(entry)
        if arxiv_id:
            return f"https://arxiv.org/pdf/{arxiv_id}.pdf"
        return ""

    # ---------- PDF download: robust + validated ----------
    def _get_pdf_path(self, arxiv_id: str) -> Path:
        safe_filename = arxiv_id.replace("/", "_") + ".pdf"
        return self.pdf_cache_dir / safe_filename

    async def download_pdf(self, paper: ArxivPaper, force_download: bool = False) -> Optional[Path]:
        if not paper.pdf_url:
            logger.error(f"No PDF URL for paper {paper.arxiv_id}")
            return None

        pdf_path = self._get_pdf_path(paper.arxiv_id)
        if pdf_path.exists() and not force_download:
            logger.info(f"Using cached PDF: {pdf_path}")
            return pdf_path

        # Acquire semaphore to limit concurrent downloads
        async with self._download_semaphore:
            success = await self._download_with_retry_validated(paper.pdf_url, pdf_path)
            return pdf_path if success else None

    async def _download_with_retry_validated(self, url: str, path: Path) -> bool:
        max_retries = int(self._settings.download_max_retries or 3)
        base_delay = float(self._settings.download_retry_delay_base or 2.0)
        headers = {"User-Agent": "arXivIngestBot/1.0"}

        # small helper = attempt to download & validate
        async def _attempt_once():
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    content_type = response.headers.get("Content-Type", "")
                    if "pdf" not in content_type.lower():
                        raise PDFDownloadException(f"URL did not return PDF (Content-Type={content_type})")

                    # stream to bytes (but avoid huge memory if file is enormous)
                    chunks = []
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        if chunk:
                            chunks.append(chunk)
                    data = b"".join(chunks)

                    if not _is_pdf_bytes(data):
                        raise PDFDownloadException("Downloaded content is not a valid PDF (missing %PDF-)")

                    # success -> write to disk
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(data)
                    return True

        attempt = 0
        while attempt < max_retries:
            try:
                attempt += 1
                # Wait small exponential backoff between retries
                if attempt > 1:
                    wait = base_delay * (2 ** (attempt - 2))
                    logger.info(f"Retrying download in {wait:.1f}s (attempt {attempt}/{max_retries})")
                    await asyncio.sleep(wait)
                result = await _attempt_once()
                if result:
                    logger.info(f"Successfully downloaded PDF to {path}")
                    return True
            except httpx.TimeoutException as e:
                logger.warning(f"Timeout during PDF download (attempt {attempt}/{max_retries}): {e}")
                if attempt >= max_retries:
                    raise PDFDownloadTimeoutError(f"Timed out after {max_retries} attempts: {e}")
            except PDFDownloadException as e:
                logger.error(f"PDF validation failed: {e}")
                # these are usually not transient, so break out
                break
            except httpx.HTTPError as e:
                logger.warning(f"HTTP error during download (attempt {attempt}/{max_retries}): {e}")
                if attempt >= max_retries:
                    raise PDFDownloadException(str(e))
            except Exception as e:
                logger.exception(f"Unexpected error while downloading PDF: {e}")
                if attempt >= max_retries:
                    raise PDFDownloadException(str(e))

        # cleanup partial file
        if path.exists():
            try:
                path.unlink()
            except Exception:
                logger.debug("Failed to remove partial file during cleanup")
        return False
