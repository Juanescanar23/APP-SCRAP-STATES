from __future__ import annotations

import re
from difflib import SequenceMatcher


DOMAIN_WORD_RE = re.compile(r"[^a-z0-9]+")


def tokenize(value: str) -> set[str]:
    return {token for token in DOMAIN_WORD_RE.split(value.casefold()) if token}


def compact(value: str) -> str:
    return "".join(token for token in DOMAIN_WORD_RE.split(value.casefold()) if token)


def overlap_score(left: str, right: str) -> float:
    left_tokens = tokenize(left)
    right_tokens = tokenize(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def string_score(left: str, right: str) -> float:
    return SequenceMatcher(None, left.casefold(), right.casefold()).ratio()


def score_candidate_domain(entity_name: str, domain: str, *, location_hint: str | None = None) -> float:
    hostname = domain.removeprefix("https://").removeprefix("http://").split("/", 1)[0]
    root_label = hostname.split(".", 1)[0]
    compact_entity = compact(entity_name)
    compact_root = compact(root_label)

    name_score = overlap_score(entity_name, root_label)
    sequence_score = string_score(compact_entity, compact_root)
    exact_compact_score = 1.0 if compact_entity and compact_entity == compact_root else 0.0
    contains_score = (
        1.0
        if compact_entity and compact_root and (compact_entity in compact_root or compact_root in compact_entity)
        else 0.0
    )
    location_score = overlap_score(location_hint or "", hostname) if location_hint else 0.0

    score = (
        (name_score * 0.20)
        + (sequence_score * 0.45)
        + (exact_compact_score * 0.25)
        + (contains_score * 0.05)
        + (location_score * 0.05)
    )
    return round(min(score, 1.0), 4)
