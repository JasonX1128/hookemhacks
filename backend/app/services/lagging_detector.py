from __future__ import annotations

from backend.app.models.contracts import RelatedMarket


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def annotate_market_status(
    market: RelatedMarket,
    *,
    category_score: float = 0.0,
    semantic_similarity: float = 0.0,
    historical_comovement: float = 0.0,
    cointegration_bonus: float = 0.0,
) -> RelatedMarket:
    expected = market.expectedReactionScore or 0.0
    residual = abs(market.residualZscore or 0.0)
    relation_confidence = _clamp(
        0.35 * market.relationStrength
        + 0.25 * category_score
        + 0.2 * semantic_similarity
        + 0.2 * historical_comovement
    )
    lagging_score = _clamp(
        expected
        * (0.5 + 0.5 * relation_confidence)
        * max(0.0, residual - 1.0)
        / 1.1
    )
    divergence_score = _clamp(
        (
            max(0.0, residual - 2.1) / 0.9
        )
        * (0.65 + 0.35 * (1.0 - min(expected, historical_comovement)))
        - 0.5 * cointegration_bonus
    )

    if divergence_score >= 0.55:
        market.status = "divergent"
        market.note = (
            market.note
            or "Worth checking: this market is moving unusually far from its usual macro relationship to the primary move."
        )
    elif lagging_score >= 0.38:
        market.status = "possibly_lagging"
        market.note = (
            market.note
            or "Worth checking: this market usually follows the same macro shock, but the follow-through still looks muted."
        )
    else:
        market.status = market.status or "normal"
        market.note = market.note or "Related market with a plausible macro linkage to the same shock."

    return market
