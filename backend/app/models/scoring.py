from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CatalystScoreBreakdown:
    time_proximity: float
    semantic_relevance: float
    event_importance: float
    source_agreement: float
    move_alignment: float

    @property
    def total(self) -> float:
        weighted = (
            0.24 * self.time_proximity
            + 0.28 * self.semantic_relevance
            + 0.18 * self.event_importance
            + 0.15 * self.source_agreement
            + 0.15 * self.move_alignment
        )
        return round(max(0.0, min(1.0, weighted)), 4)


@dataclass(slots=True)
class RelatedMarketScoreBreakdown:
    category_score: float
    topic_score: float
    semantic_similarity: float
    historical_comovement: float
    cointegration_bonus: float = 0.0

    @property
    def total(self) -> float:
        weighted = (
            0.25 * self.category_score
            + 0.15 * self.topic_score
            + 0.3 * self.semantic_similarity
            + 0.25 * self.historical_comovement
            + 0.05 * self.cointegration_bonus
        )
        return round(max(0.0, min(1.0, weighted)), 4)
