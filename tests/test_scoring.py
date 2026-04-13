from __future__ import annotations

from app.services.scoring import score_candidate_domain


def test_score_candidate_domain_prefers_name_overlap() -> None:
    strong = score_candidate_domain("sunrise labs", "sunriselabs.com")
    weak = score_candidate_domain("sunrise labs", "completelydifferent.org")

    assert strong > weak
    assert strong > 0.5

