from __future__ import annotations

import logging

from backend.app.core.config import get_settings
from backend.app.models.contracts import AttributionResponse, MarketClickContext, MoveSummary
from backend.app.services.catalyst_retrieval import CatalystRetrievalService
from backend.app.services.catalyst_scoring import CatalystScoringService
from backend.app.services.catalyst_synthesis import CatalystSynthesisService
from backend.app.services.move_analyzer import MoveAnalyzer
from backend.app.services.news_search import NewsSearchService
from backend.app.services.utils import clamp_score
from backend.app.services.related_markets import RelatedMarketsService
from backend.app.storage.cache_repo import CacheRepository

logger = logging.getLogger(__name__)


def _fallback_confidence(context: MarketClickContext) -> float:
    fallback_price = context.clickedPrice if context.clickedPrice is not None else 0.5
    price_before = context.priceBefore if context.priceBefore is not None else max(0.0, fallback_price - 0.08)
    price_after = context.priceAfter if context.priceAfter is not None else fallback_price
    return clamp_score(0.28 + abs(price_after - price_before) * 0.9)


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


class AttributionService:
    def __init__(self) -> None:
        settings = get_settings()
        cache_repo = CacheRepository()
        self.move_analyzer = MoveAnalyzer()
        self.catalyst_retrieval = CatalystRetrievalService()
        self.catalyst_scoring = CatalystScoringService()
        self.related_markets = RelatedMarketsService(cache_repo)
        self.news_search = NewsSearchService(api_key=settings.serper_api_key)
        self.catalyst_synthesis = CatalystSynthesisService(
            project_id=settings.vertex_project_id,
            location=settings.vertex_location,
        )
        self._mock_mode = settings.mock_mode

    def attribute_move(self, context: MarketClickContext) -> AttributionResponse:
        try:
            move_summary = self.move_analyzer.characterize_move(context).summary
        except Exception:
            logger.exception("Falling back to a flat move summary after move analysis failed.")
            move_summary = _fallback_move_summary(context)

        try:
            raw_candidates = self.catalyst_retrieval.retrieve(context, move_summary)
        except Exception:
            logger.exception("Continuing without retrieved catalyst candidates after retrieval failed.")
            raw_candidates = []

        try:
            ranked_candidates = self.catalyst_scoring.score(
                context=context,
                move_summary=move_summary,
                candidates=raw_candidates,
            )
        except Exception:
            logger.exception("Continuing without ranked catalyst candidates after scoring failed.")
            ranked_candidates = []
        top_catalyst = ranked_candidates[0] if ranked_candidates else None
        alternative_catalysts = ranked_candidates[1:4]
        try:
            evidence = self.catalyst_scoring.select_evidence(
                top_catalyst=top_catalyst,
                ranked_candidates=ranked_candidates,
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

        try:
            related_markets = self.related_markets.find_related_markets(context)
        except Exception:
            logger.exception("Continuing without related markets after related-market lookup failed.")
            related_markets = []

        synthesized_catalyst = None
        synthesized_evidence = []

        print(f"[DEBUG] Mock mode: {self._mock_mode}")
        if not self._mock_mode:
            try:
                print("[DEBUG] Searching for articles...")
                articles = self.news_search.search_sync(context)
                print(f"[DEBUG] Found {len(articles)} articles")
                if articles:
                    print("[DEBUG] Starting synthesis...")
                    synthesized_catalyst, relevant_articles = self.catalyst_synthesis.synthesize(
                        context=context,
                        move=move_summary,
                        articles=articles,
                    )
                    print(f"[DEBUG] Synthesis result: {synthesized_catalyst}")
                    synthesized_evidence = self.catalyst_synthesis.articles_to_evidence(relevant_articles)
            except Exception as e:
                print(f"[DEBUG] Synthesis error: {e}")
                logger.exception("Continuing without synthesized catalyst after synthesis failed.")

        return AttributionResponse(
            primaryMarket=context,
            moveSummary=move_summary,
            topCatalyst=top_catalyst,
            alternativeCatalysts=alternative_catalysts,
            confidence=confidence,
            evidence=evidence,
            relatedMarkets=related_markets,
            synthesizedCatalyst=synthesized_catalyst,
            synthesizedEvidence=synthesized_evidence,
        )
