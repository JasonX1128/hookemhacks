from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

STOPWORDS = {
    "and",
    "for",
    "from",
    "have",
    "into",
    "next",
    "that",
    "the",
    "this",
    "will",
    "with",
}


def clamp_score(value: float, *, digits: int = 4) -> float:
    return round(max(0.0, min(1.0, value)), digits)


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def shift_timestamp(value: str, *, minutes: int) -> str:
    shifted = parse_timestamp(value) + timedelta(minutes=minutes)
    return shifted.isoformat().replace("+00:00", "Z")


def slugify(*parts: str) -> str:
    raw_value = "-".join(part for part in parts if part)
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw_value).strip("-").lower()
    return slug or "market"


def token_overlap(left: str, right: str) -> float:
    left_tokens = tokenize_text(left)
    right_tokens = tokenize_text(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens))


def tokenize_text(value: str) -> set[str]:
    return {
        token.lower()
        for token in re.findall(r"[A-Za-z0-9]+", value)
        if len(token) >= 3 and token.lower() not in STOPWORDS
    }
