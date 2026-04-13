from __future__ import annotations

import os

import pytest

from app.services.search_provider import BraveSearchProvider


@pytest.mark.asyncio
async def test_brave_search_provider_real_query() -> None:
    api_key = os.getenv("BRAVE_API_KEY")
    if not api_key:
        pytest.skip("BRAVE_API_KEY is not set.")

    provider = BraveSearchProvider(api_key)
    results = await provider.search('"OpenAI" site:openai.com', max_results=5)

    assert results
    assert any("openai.com" in result.url for result in results)

