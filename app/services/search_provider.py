from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from app.core.config import get_settings


BRAVE_WEB_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"


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


def get_search_provider() -> SearchProvider:
    settings = get_settings()
    provider_name = settings.search_provider.strip().casefold()

    if provider_name in {"", "none"}:
        return NullSearchProvider()
    if provider_name == "brave":
        if not settings.brave_search_api_key:
            raise SearchProviderError("BIZINTEL_BRAVE_SEARCH_API_KEY is required for Brave Search.")
        return BraveSearchProvider(settings.brave_search_api_key)

    raise SearchProviderError(f"Unsupported search provider: {settings.search_provider}")
