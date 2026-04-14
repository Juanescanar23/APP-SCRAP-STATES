from __future__ import annotations

from app.services.search_provider import (
    build_yahoo_query_variants,
    parse_yahoo_search_results,
)


def test_parse_yahoo_search_results_extracts_ranked_results() -> None:
    html = """
    <div id="web" class="web-res">
      <ol class="reg searchCenterMiddle">
        <li class="first">
          <div class="dd fst algo algo-sr">
            <div class="compTitle">
              <a href="https://www.knewhealth.com/">
                <h3 class="title">Medical Cost Sharing | Knew Health</h3>
              </a>
            </div>
            <div class="compText">
              <p>Join Knew Health for an affordable alternative.</p>
            </div>
          </div>
        </li>
        <li>
          <div class="dd algo algo-sr">
            <div class="compTitle">
              <a href="https://www.bizapedia.com/fl/knew-health-inc.html">
                <h3 class="title">Knew Health Inc in Florida</h3>
              </a>
            </div>
            <div class="compText">
              <p>Directory listing.</p>
            </div>
          </div>
        </li>
      </ol>
    </div>
    """

    results = parse_yahoo_search_results(html, max_results=5)

    assert len(results) == 2
    assert results[0].url == "https://www.knewhealth.com/"
    assert results[0].title == "Medical Cost Sharing | Knew Health"
    assert "affordable alternative" in results[0].snippet
    assert results[0].rank == 1
    assert results[1].rank == 2


def test_build_yahoo_query_variants_relaxes_problem_queries() -> None:
    variants = build_yahoo_query_variants('"00 PIZZA LLC" SUNRISE FL official site')

    assert variants[0] == '"00 PIZZA LLC" SUNRISE FL official site'
    assert "00 PIZZA LLC SUNRISE FL official site" in variants
    assert "00 PIZZA SUNRISE FL" in variants
    assert "00 PIZZA FL" in variants
