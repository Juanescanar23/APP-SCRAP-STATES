from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

from app.core.config import get_settings
from app.services.robots_guard import RobotsGuard


@dataclass(slots=True)
class FetchedPage:
    url: str
    html: str


@dataclass(slots=True)
class PageFetchResult:
    url: str
    html: str | None
    blocked_by_robots: bool = False


@dataclass(slots=True)
class SiteFetchOutcome:
    pages: list[FetchedPage]
    visited_urls: list[str]
    blocked_urls: list[str]


def canonical_host(url: str) -> str:
    return (urlparse(url).hostname or "").casefold().removeprefix("www.")


def build_allowlisted_urls(homepage_url: str) -> list[str]:
    parsed = urlparse(homepage_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    settings = get_settings()
    return [urljoin(base, path) for path in settings.evidence_path_allowlist]


def extract_internal_allowlisted_links(homepage_url: str, html: str) -> list[str]:
    settings = get_settings()
    parser = HTMLParser(html)
    base_host = canonical_host(homepage_url)
    links: list[str] = []

    for anchor in parser.css("a[href]"):
        href_value = anchor.attributes.get("href")
        href = href_value.strip() if isinstance(href_value, str) else ""
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        resolved = urljoin(homepage_url, href)
        parsed = urlparse(resolved)
        if parsed.scheme not in {"http", "https"}:
            continue
        if canonical_host(resolved) != base_host:
            continue

        normalized_path = (parsed.path or "/").casefold().rstrip("/") or "/"
        if normalized_path in settings.evidence_path_allowlist:
            links.append(f"{parsed.scheme}://{parsed.netloc}{normalized_path}")
            continue
        if any(keyword in normalized_path for keyword in settings.evidence_link_keywords):
            links.append(f"{parsed.scheme}://{parsed.netloc}{normalized_path}")

    return dedupe_strings(links)


async def fetch_allowlisted_site_pages(
    homepage_url: str,
    robots_guard: RobotsGuard | None = None,
    client: httpx.AsyncClient | None = None,
    *,
    max_pages: int | None = None,
) -> SiteFetchOutcome:
    guard = robots_guard or RobotsGuard(user_agent=get_settings().user_agent)
    owns_client = client is None
    http_client = client or httpx.AsyncClient(
        follow_redirects=True,
        timeout=get_settings().http_timeout_seconds,
        headers={"User-Agent": get_settings().user_agent},
    )

    visited_urls: list[str] = []
    blocked_urls: list[str] = []

    try:
        homepage_result = await fetch_page(http_client, guard, homepage_url)
        homepage_html = homepage_result.html
        if homepage_result.blocked_by_robots:
            blocked_urls.append(homepage_result.url)
        elif homepage_html is not None:
            visited_urls.append(homepage_result.url)

        seed_homepage_url = homepage_result.url if homepage_html is not None else homepage_url
        candidate_urls = build_allowlisted_urls(seed_homepage_url)
        if homepage_html is not None:
            candidate_urls.extend(
                extract_internal_allowlisted_links(seed_homepage_url, homepage_html)
            )

        limit = max_pages or get_settings().evidence_max_pages
        candidate_urls = dedupe_strings(candidate_urls)[:limit]
        follow_up_results = await asyncio.gather(
            *[
                fetch_page(http_client, guard, url)
                for url in candidate_urls
                if url != homepage_result.url
            ],
        )
    finally:
        if owns_client:
            await http_client.aclose()

    pages: list[FetchedPage] = []
    if homepage_html is not None:
        pages.append(FetchedPage(url=homepage_result.url, html=homepage_html))

    for result in follow_up_results:
        if result.blocked_by_robots:
            blocked_urls.append(result.url)
        elif result.html is not None:
            visited_urls.append(result.url)
            pages.append(FetchedPage(url=result.url, html=result.html))

    return SiteFetchOutcome(
        pages=pages,
        visited_urls=dedupe_strings(visited_urls),
        blocked_urls=dedupe_strings(blocked_urls),
    )


async def fetch_page(
    client: httpx.AsyncClient,
    guard: RobotsGuard,
    url: str,
) -> PageFetchResult:
    decision = await guard.check(url)
    if not decision.allowed:
        return PageFetchResult(url=url, html=None, blocked_by_robots=True)

    if decision.crawl_delay:
        await asyncio.sleep(min(decision.crawl_delay, 10.0))
    elif decision.request_rate:
        requests, seconds = decision.request_rate
        if requests > 0:
            await asyncio.sleep(min(seconds / requests, 10.0))

    try:
        response = await client.get(url)
    except httpx.HTTPError:
        return PageFetchResult(url=url, html=None)

    if canonical_host(str(response.url)) != canonical_host(url):
        return PageFetchResult(url=str(response.url), html=None)
    if response.status_code >= 400 or "text/html" not in response.headers.get("content-type", ""):
        return PageFetchResult(url=str(response.url), html=None)

    return PageFetchResult(url=str(response.url), html=response.text)


def dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped
