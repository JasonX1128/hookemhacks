from __future__ import annotations

from typing import Any

from backend.app.models.contracts import CatalystCandidate, MarketClickContext, MoveSummary, RetrievedCatalystCandidate
from backend.app.services.catalyst_scoring import CatalystScoringService


def rank_candidates(
    context: MarketClickContext,
    move_summary: MoveSummary,
    raw_candidates: list[dict[str, Any]],
) -> list[CatalystCandidate]:
    return CatalystScoringService().score(
        context=context,
        move_summary=move_summary,
        candidates=[RetrievedCatalystCandidate.model_validate(candidate) for candidate in raw_candidates],
    )
