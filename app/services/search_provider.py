from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx
from selectolax.parser import HTMLParser

from app.core.config import get_settings

BRAVE_WEB_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
YAHOO_WEB_SEARCH_URL = "https://search.yahoo.com/search"
YAHOO_QUERY_NOISE_TOKENS = {"official", "site", "contact"}
YAHOO_LEGAL_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "lp",
    "ltd",
    "limited",
    "pllc",
}


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
            had_successful_response = False
            last_error: httpx.HTTPError | None = None

            for query_variant in build_yahoo_query_variants(query):
                try:
                    response = await client.get(
                        YAHOO_WEB_SEARCH_URL,
                        headers={
                            "Accept": "text/html,application/xhtml+xml",
                            "User-Agent": settings.user_agent,
                        },
                        params={"p": query_variant},
                    )
                    response.raise_for_status()
                except httpx.HTTPError as exc:
                    last_error = exc
                    continue

                had_successful_response = True
                results = parse_yahoo_search_results(response.text, max_results=max_results)
                if results:
                    return results

            if had_successful_response:
                return []
            if last_error is not None:
                raise SearchProviderError(
                    f"Search provider request failed: {last_error}"
                ) from last_error
            return []
        finally:
            if owns_client:
                await client.aclose()


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


def build_yahoo_query_variants(query: str) -> list[str]:
    normalized = " ".join(query.split())
    variants: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        collapsed = " ".join(candidate.split()).strip()
        if collapsed and collapsed not in seen:
            seen.add(collapsed)
            variants.append(collapsed)

    add(normalized)

    dequoted = normalized.replace('"', "")
    add(dequoted)

    stripped_noise_tokens = [
        token for token in dequoted.split() if token.casefold() not in YAHOO_QUERY_NOISE_TOKENS
    ]
    add(" ".join(stripped_noise_tokens))

    core_tokens = [
        token for token in stripped_noise_tokens if token.casefold() not in YAHOO_LEGAL_SUFFIXES
    ]
    add(" ".join(core_tokens))

    state_token = ""
    if core_tokens and _looks_like_state_token(core_tokens[-1]):
        state_token = core_tokens[-1]

    business_tokens = [token for token in core_tokens if token != state_token]
    if business_tokens:
        add(" ".join(business_tokens[:3]))
        if state_token:
            add(" ".join([*business_tokens[:2], state_token]))

    return variants


def _looks_like_state_token(token: str) -> bool:
    return len(token) == 2 and token.isalpha() and token.upper() == token


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
