from __future__ import annotations

from app.services.site_fetch import extract_internal_allowlisted_links


def test_extract_internal_allowlisted_links_limits_to_same_domain() -> None:
    html = """
    <html>
      <body>
        <a href="/contact">Contact</a>
        <a href="https://example.com/privacy">Privacy</a>
        <a href="https://linkedin.com/company/example">LinkedIn</a>
      </body>
    </html>
    """

    links = extract_internal_allowlisted_links("https://example.com", html)

    assert "https://example.com/contact" in links
    assert "https://example.com/privacy" in links
    assert all("linkedin.com" not in link for link in links)
