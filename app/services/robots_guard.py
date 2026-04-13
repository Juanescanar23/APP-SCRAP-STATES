from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RequestRate, RobotFileParser

import httpx


@dataclass(slots=True)
class RobotsDecision:
    allowed: bool
    crawl_delay: float | None
    request_rate: tuple[int, int] | None


class RobotsGuard:
    def __init__(self, user_agent: str = "bizintel-bot/0.1") -> None:
        self.user_agent = user_agent
        self._cache: dict[str, RobotFileParser] = {}

    async def check(self, target_url: str) -> RobotsDecision:
        parsed = urlparse(target_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        robots_url = urljoin(root, "/robots.txt")

        rp = self._cache.get(root)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
                    response = await client.get(robots_url)
                    if response.status_code < 400:
                        rp.parse(response.text.splitlines())
                    else:
                        rp.parse([])
            except httpx.HTTPError:
                rp.parse([])
            self._cache[root] = rp

        request_rate = rp.request_rate(self.user_agent)
        return RobotsDecision(
            allowed=rp.can_fetch(self.user_agent, target_url),
            crawl_delay=rp.crawl_delay(self.user_agent),
            request_rate=_format_request_rate(request_rate),
        )


def _format_request_rate(value: RequestRate | None) -> tuple[int, int] | None:
    if value is None:
        return None
    return (value.requests, value.seconds)

