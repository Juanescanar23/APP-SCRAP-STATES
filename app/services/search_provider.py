from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx
from selectolax.parser import HTMLParser

from app.core.config import get_settings

BRAVE_WEB_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
YAHOO_WEB_SEARCH_URL = "https://search.yahoo.com/search"


class SearchProviderError(RuntimeError):
    pass


@dataclass(slots=True)
class SearchResult:
    url: str
    title: str
    snippet: str
    rank: int
    provider: str
    raw: dict[str, Any]


class SearchProvider(Protocol):
    provider_name: str

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        raise NotImplementedError


class NullSearchProvider:
    provider_name = "none"

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        return []


class FakeSearchProvider:
    provider_name = "fake"

    def __init__(self, results_by_query: dict[str, list[SearchResult]] | None = None) -> None:
        self._results_by_query = results_by_query or {}

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        return self._results_by_query.get(query, [])[:max_results]


class BraveSearchProvider:
    provider_name = "brave"

    def __init__(self, api_key: str, *, client: httpx.AsyncClient | None = None) -> None:
        self.api_key = api_key
        self._client = client

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        settings = get_settings()
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=settings.http_timeout_seconds)

        try:
            response = await client.get(
                BRAVE_WEB_SEARCH_URL,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": self.api_key,
                },
                params={
                    "q": query,
                    "count": max_results,
                    "country": "us",
                    "search_lang": "en",
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise SearchProviderError(f"Search provider request failed: {exc}") from exc
        finally:
            if owns_client:
                await client.aclose()

        payload = response.json()
        results = payload.get("web", {}).get("results", [])
        normalized: list[SearchResult] = []
        for rank, item in enumerate(results, start=1):
            normalized.append(
                SearchResult(
                    url=str(item.get("url") or ""),
                    title=str(item.get("title") or ""),
                    snippet=str(item.get("description") or ""),
                    rank=rank,
                    provider=self.provider_name,
                    raw=item,
                ),
            )
        return normalized


class YahooSearchProvider:
    provider_name = "yahoo"

    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._client = client

    async def search(self, query: str, *, max_results: int) -> list[SearchResult]:
        settings = get_settings()
        owns_client = self._client is None
        client = self._client or httpx.AsyncClient(
            timeout=settings.http_timeout_seconds,
            follow_redirects=True,
        )

        try:
            response = await client.get(
                YAHOO_WEB_SEARCH_URL,
                headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": settings.user_agent,
                },
                params={"p": query},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise SearchProviderError(f"Search provider request failed: {exc}") from exc
        finally:
            if owns_client:
                await client.aclose()

        return parse_yahoo_search_results(response.text, max_results=max_results)


def parse_yahoo_search_results(html: str, *, max_results: int) -> list[SearchResult]:
    parser = HTMLParser(html)
    normalized: list[SearchResult] = []

    for item in parser.css("#web ol.reg li div.algo"):
        anchor = item.css_first("div.compTitle a[href]")
        if anchor is None:
            continue

        url = anchor.attributes.get("href", "").strip()
        if not url or _is_search_host(url):
            continue

        title_node = item.css_first("h3")
        snippet_node = item.css_first("div.compText p")
        title = title_node.text(strip=True) if title_node else anchor.text(strip=True)
        snippet = snippet_node.text(separator=" ", strip=True) if snippet_node else ""

        normalized.append(
            SearchResult(
                url=url,
                title=title,
                snippet=snippet,
                rank=len(normalized) + 1,
                provider="yahoo",
                raw={},
            ),
        )
        if len(normalized) >= max_results:
            break

    return normalized


def _is_search_host(url: str) -> bool:
    hostname = (urlparse(url).hostname or "").casefold()
    return hostname == "search.yahoo.com" or hostname.endswith(".search.yahoo.com")


def get_search_provider() -> SearchProvider:
    settings = get_settings()
    provider_name = settings.search_provider.strip().casefold()

    if provider_name in {"", "none"}:
        return NullSearchProvider()
    if provider_name == "brave":
        if not settings.brave_search_api_key:
            raise SearchProviderError("BIZINTEL_BRAVE_SEARCH_API_KEY is required for Brave Search.")
        return BraveSearchProvider(settings.brave_search_api_key)
    if provider_name in {"yahoo", "yahoo_html"}:
        return YahooSearchProvider()

    raise SearchProviderError(f"Unsupported search provider: {settings.search_provider}")
