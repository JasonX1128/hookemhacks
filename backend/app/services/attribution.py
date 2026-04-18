from __future__ import annotations

from dataclasses import dataclass, field
import logging

from ..schemas.contracts import AttributionResponse, MarketClickContext, MoveSummary
from .catalyst_retrieval import CatalystRetrievalService
from .catalyst_scoring import CatalystScoringService
from .move_analyzer import MoveAnalyzer
from .propagation import PropagationService
from .related_markets import RelatedMarketsService
from .utils import clamp_score

logger = logging.getLogger(__name__)


def _fallback_move_summary(context: MarketClickContext) -> MoveSummary:
    fallback_price = context.clickedPrice if context.clickedPrice is not None else 0.5
    price_before = context.priceBefore if context.priceBefore is not None else max(0.0, fallback_price - 0.08)
    price_after = context.priceAfter if context.priceAfter is not None else fallback_price
    delta = price_after - price_before
    move_direction = "flat"
    if delta > 0.01:
        move_direction = "up"
    elif delta < -0.01:
        move_direction = "down"

    return MoveSummary(
        moveMagnitude=round(abs(delta), 2),
        moveDirection=move_direction,
        jumpScore=clamp_score(0.18 + abs(delta) * 3.2, digits=2),
    )


def _fallback_confidence(context: MarketClickContext) -> float:
    fallback_price = context.clickedPrice if context.clickedPrice is not None else 0.5
    price_before = context.priceBefore if context.priceBefore is not None else max(0.0, fallback_price - 0.08)
    price_after = context.priceAfter if context.priceAfter is not None else fallback_price
    return clamp_score(0.28 + abs(price_after - price_before) * 0.9)


@dataclass(slots=True)
class AttributionService:
    catalyst_retrieval: CatalystRetrievalService
    catalyst_scoring: CatalystScoringService
    related_markets: RelatedMarketsService
    propagation: PropagationService
    move_analyzer: MoveAnalyzer = field(default_factory=MoveAnalyzer)

    def attribute_move(self, context: MarketClickContext) -> AttributionResponse:
        try:
            move_summary = self.move_analyzer.characterize_move(context).summary
        except Exception:
            logger.exception("Falling back to a best-effort move summary after move analysis failed.")
            move_summary = _fallback_move_summary(context)

        try:
            raw_candidates = self.catalyst_retrieval.retrieve(context, move_summary)
        except Exception:
            logger.exception("Continuing without retrieved catalyst candidates after retrieval failed.")
            raw_candidates = []

        try:
            scored_candidates = self.catalyst_scoring.score(
                context=context,
                move_summary=move_summary,
                candidates=raw_candidates,
            )
        except Exception:
            logger.exception("Continuing without ranked catalyst candidates after scoring failed.")
            scored_candidates = []

        try:
            related_markets = self.related_markets.find_related_markets(context)
        except Exception:
            logger.exception("Continuing without related markets after related-market lookup failed.")
            related_markets = []

        try:
            propagated_markets = self.propagation.propagate_to_related_markets(
                move_summary=move_summary,
                related_markets=related_markets,
            )
        except Exception:
            logger.exception("Using unpropagated related markets after propagation failed.")
            propagated_markets = related_markets
        top_catalyst = scored_candidates[0] if scored_candidates else None
        alternative_catalysts = scored_candidates[1:3]
        try:
            evidence = self.catalyst_scoring.select_evidence(
                top_catalyst=top_catalyst,
                ranked_candidates=scored_candidates,
            )
        except Exception:
            logger.exception("Continuing with a reduced evidence set after evidence selection failed.")
            evidence = [top_catalyst] if top_catalyst is not None else []

        try:
            confidence = self.catalyst_scoring.compute_confidence(
                move_summary=move_summary,
                top_catalyst=top_catalyst,
                alternative_catalysts=alternative_catalysts,
                evidence=evidence,
            )
        except Exception:
            logger.exception("Using fallback confidence after confidence scoring failed.")
            confidence = _fallback_confidence(context)
        return AttributionResponse(
            primaryMarket=context,
            moveSummary=move_summary,
            topCatalyst=top_catalyst,
            alternativeCatalysts=alternative_catalysts,
            confidence=confidence,
            evidence=evidence,
            relatedMarkets=propagated_markets,
        )
