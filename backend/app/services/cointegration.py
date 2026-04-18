from __future__ import annotations

from collections.abc import Collection

COINTEGRATION_ENABLED = True


def score_cointegration_pair(
    _primary_market_id: str,
    _related_market_id: str,
    *,
    enough_history: bool,
    category_score: float = 0.0,
    topic_score: float = 0.0,
    semantic_similarity: float = 0.0,
    historical_comovement: float = 0.0,
    primary_topics: Collection[str] | None = None,
    related_topics: Collection[str] | None = None,
) -> float:
    # Keep this intentionally small and gated. It is only a lightweight stability
    # hint for already-plausible pairs, not a standalone trading or arbitrage claim.
    if not COINTEGRATION_ENABLED or not enough_history:
        return 0.0

    shared_topics = set(primary_topics or ()) & set(related_topics or ())
    plausible_pair = historical_comovement >= 0.72 and (
        category_score >= 0.72
        or topic_score >= 0.68
        or semantic_similarity >= 0.68
        or bool(shared_topics)
    )
    if not plausible_pair:
        return 0.0

    stability = (
        0.45 * historical_comovement
        + 0.25 * category_score
        + 0.2 * topic_score
        + 0.1 * semantic_similarity
    )
    return round(max(0.0, min(0.12, stability - 0.7)), 4)
