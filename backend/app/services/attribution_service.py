from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from backend.app.core.config import get_settings
from backend.app.models.contracts import (
    AttributionResponse,
    AttributionSynthesisResponse,
    CatalystCandidate,
    EvidenceSource,
    MarketClickContext,
    MoveSummary,
    RelatedMarket,
    SynthesizedCatalyst,
)
from backend.app.services.catalyst_retrieval import CatalystRetrievalService
from backend.app.services.catalyst_scoring import CatalystScoringService
from backend.app.services.catalyst_synthesis import CatalystSynthesisService
from backend.app.services.market_context import MarketContextService
from backend.app.services.market_data import MarketDataService
from backend.app.services.move_analyzer import MoveAnalyzer
from backend.app.services.news_search import NewsSearchService
from backend.app.services.related_markets import RelatedMarketsService
from backend.app.services.utils import clamp_score
from backend.app.storage.cache_repo import CacheRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreparedContext:
    context: MarketClickContext
    move_summary: MoveSummary
    data_quality: float


@dataclass(frozen=True)
class PreparedAttribution:
    context: MarketClickContext
    move_summary: MoveSummary
    top_catalyst: CatalystCandidate | None
    alternative_catalysts: list[CatalystCandidate]
    evidence: list[CatalystCandidate]
    confidence: float
    data_quality: float


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


def _blend_confidence(base_confidence: float, synthesized_confidence: float | None) -> float:
    if synthesized_confidence is None:
        return base_confidence

    blended = clamp_score(0.65 * base_confidence + 0.35 * synthesized_confidence)
    return min(base_confidence, blended)


def _context_data_quality(context: MarketClickContext) -> float:
    if context.priceBefore is not None and context.priceAfter is not None:
        return 0.55
    if (context.priceBefore is not None or context.priceAfter is not None) and context.clickedPrice is not None:
        return 0.42
    if context.clickedPrice is not None:
        return 0.32
    return 0.18


class AttributionService:
    def __init__(self) -> None:
        settings = get_settings()
        cache_repo = CacheRepository()
        self.market_context = MarketContextService()
        self.market_data = MarketDataService(cache_repo)
        self.move_analyzer = MoveAnalyzer()
        self.catalyst_retrieval = CatalystRetrievalService()
        self.catalyst_scoring = CatalystScoringService()
        self.related_markets = RelatedMarketsService(cache_repo)
        self.news_search = NewsSearchService(api_key=settings.serper_api_key)
        self.catalyst_synthesis = CatalystSynthesisService(
            project_id=settings.vertex_project_id if not settings.mock_mode else None,
            location=settings.vertex_location,
        )
        self._mock_mode = settings.mock_mode

    def attribute_move(self, context: MarketClickContext) -> AttributionResponse:
        pipeline_start = time.perf_counter()
        prepared = self._prepare_attribution(context)

        related_markets = []
        synthesized_catalyst = None
        synthesized_evidence = []
        confidence = prepared.confidence

        if self._mock_mode:
            logger.debug("[7/8] Finding related markets (mock mode, skipping synthesis)")
            related_markets = self._find_related_markets_safe(prepared.context)
            logger.debug("[8/8] Skipped (mock mode)")
        else:
            logger.debug("[7/8 + 8/8] Running related markets and synthesis in parallel")
            parallel_start = time.perf_counter()

            with ThreadPoolExecutor(max_workers=2) as executor:
                related_future = executor.submit(self._find_related_markets_safe, prepared.context)
                synthesis_future = executor.submit(
                    self._run_synthesis_pipeline,
                    prepared.context,
                    prepared.move_summary,
                )

                related_markets = related_future.result()
                synth_result = synthesis_future.result()
                synthesized_catalyst = synth_result[0]
                synthesized_evidence = synth_result[1]

            logger.debug(
                "[7/8 + 8/8] Parallel tasks completed in %.2fs (related: %d, synthesis: %s)",
                time.perf_counter() - parallel_start,
                len(related_markets),
                "success" if synthesized_catalyst else "none",
            )

            if synthesized_catalyst is not None:
                confidence = _blend_confidence(prepared.confidence, synthesized_catalyst.confidence)
                logger.debug("Blended confidence: %.2f", confidence)

        total_time = time.perf_counter() - pipeline_start
        logger.info(
            "Attribution pipeline completed in %.2fs for %s (%s %.1f%%)",
            total_time,
            prepared.context.marketId,
            prepared.move_summary.moveDirection,
            prepared.move_summary.moveMagnitude * 100,
        )

        return self._build_attribution_response(
            prepared=prepared,
            related_markets=related_markets,
            confidence=confidence,
            synthesized_catalyst=synthesized_catalyst,
            synthesized_evidence=synthesized_evidence,
        )

    def attribute_move_overview(self, context: MarketClickContext) -> AttributionResponse:
        pipeline_start = time.perf_counter()
        prepared = self._prepare_attribution(context)
        logger.debug("[7/7] Finding related markets for overview response")
        related_markets = self._find_related_markets_safe(prepared.context)
        total_time = time.perf_counter() - pipeline_start
        logger.info(
            "Overview attribution completed in %.2fs for %s (%s %.1f%%)",
            total_time,
            prepared.context.marketId,
            prepared.move_summary.moveDirection,
            prepared.move_summary.moveMagnitude * 100,
        )
        return self._build_attribution_response(prepared=prepared, related_markets=related_markets)

    def attribute_move_synthesis(self, context: MarketClickContext) -> AttributionSynthesisResponse:
        pipeline_start = time.perf_counter()
        if self._mock_mode:
            logger.debug("[synthesis] Skipping AI synthesis in mock mode")
            return AttributionSynthesisResponse()

        prepared_context = self._prepare_context_and_move(context)
        synthesized_catalyst, synthesized_evidence = self._run_synthesis_pipeline(
            prepared_context.context,
            prepared_context.move_summary,
        )
        logger.info(
            "Synthesis-only attribution completed in %.2fs for %s",
            time.perf_counter() - pipeline_start,
            prepared_context.context.marketId,
        )
        return AttributionSynthesisResponse(
            synthesizedCatalyst=synthesized_catalyst,
            synthesizedEvidence=synthesized_evidence,
        )

    def _prepare_context_and_move(self, context: MarketClickContext) -> PreparedContext:
        logger.debug("[1/8] Hydrating market context for %s", context.marketId)
        step_start = time.perf_counter()
        context = self.market_context.hydrate_context(context)
        logger.debug("[1/8] Context hydrated in %.2fs", time.perf_counter() - step_start)

        logger.debug("[2/8] Fetching real market data from Kalshi API")
        step_start = time.perf_counter()
        move_data_source = "fallback"
        data_quality = _context_data_quality(context)
        try:
            real_move = self.market_data.compute_real_move(context)
            if real_move:
                move_summary = self.market_data.to_move_summary(real_move)
                move_data_source = real_move.data_source
                data_quality = clamp_score(real_move.confidence, digits=2)
                logger.debug(
                    "[2/8] Real move data from %s in %.2fs: %s %.1f%% (confidence=%.1f)",
                    move_data_source,
                    time.perf_counter() - step_start,
                    move_summary.moveDirection,
                    move_summary.moveMagnitude * 100,
                    real_move.confidence,
                )
            else:
                move_summary = _fallback_move_summary(context)
                logger.debug("[2/8] No real data available, using fallback move summary")
        except Exception:
            logger.exception("Falling back to default move summary after market data fetch failed.")
            move_summary = _fallback_move_summary(context)
            logger.debug(
                "[2/8] Fallback move in %.2fs: %s %.1f%%",
                time.perf_counter() - step_start,
                move_summary.moveDirection,
                move_summary.moveMagnitude * 100,
            )

        return PreparedContext(
            context=context,
            move_summary=move_summary,
            data_quality=data_quality,
        )

    def _prepare_attribution(self, context: MarketClickContext) -> PreparedAttribution:
        prepared_context = self._prepare_context_and_move(context)

        logger.debug("[3/8] Retrieving catalyst candidates")
        step_start = time.perf_counter()
        try:
            raw_candidates = self.catalyst_retrieval.retrieve(prepared_context.context, prepared_context.move_summary)
        except Exception:
            logger.exception("Continuing without retrieved catalyst candidates after retrieval failed.")
            raw_candidates = []
        logger.debug("[3/8] Retrieved %d candidates in %.2fs", len(raw_candidates), time.perf_counter() - step_start)

        logger.debug("[4/8] Scoring catalyst candidates")
        step_start = time.perf_counter()
        try:
            ranked_candidates = self.catalyst_scoring.score(
                context=prepared_context.context,
                move_summary=prepared_context.move_summary,
                candidates=raw_candidates,
            )
        except Exception:
            logger.exception("Continuing without ranked catalyst candidates after scoring failed.")
            ranked_candidates = []
        logger.debug("[4/8] Scored %d candidates in %.2fs", len(ranked_candidates), time.perf_counter() - step_start)

        top_catalyst = ranked_candidates[0] if ranked_candidates else None
        alternative_catalysts = ranked_candidates[1:4]

        logger.debug("[5/8] Selecting evidence")
        step_start = time.perf_counter()
        try:
            evidence = self.catalyst_scoring.select_evidence(
                top_catalyst=top_catalyst,
                ranked_candidates=ranked_candidates,
            )
        except Exception:
            logger.exception("Continuing with a reduced evidence set after evidence selection failed.")
            evidence = [top_catalyst] if top_catalyst is not None else []
        logger.debug("[5/8] Selected %d evidence items in %.2fs", len(evidence), time.perf_counter() - step_start)

        logger.debug("[6/8] Computing confidence score")
        step_start = time.perf_counter()
        try:
            confidence = self.catalyst_scoring.compute_confidence(
                move_summary=prepared_context.move_summary,
                top_catalyst=top_catalyst,
                alternative_catalysts=alternative_catalysts,
                evidence=evidence,
            )
        except Exception:
            logger.exception("Using fallback confidence after confidence scoring failed.")
            confidence = _fallback_confidence(prepared_context.context)
        logger.debug("[6/8] Confidence %.2f computed in %.2fs", confidence, time.perf_counter() - step_start)

        return PreparedAttribution(
            context=prepared_context.context,
            move_summary=prepared_context.move_summary,
            top_catalyst=top_catalyst,
            alternative_catalysts=alternative_catalysts,
            evidence=evidence,
            confidence=confidence,
            data_quality=prepared_context.data_quality,
        )

    def _build_attribution_response(
        self,
        prepared: PreparedAttribution,
        related_markets: list[RelatedMarket],
        confidence: float | None = None,
        synthesized_catalyst: SynthesizedCatalyst | None = None,
        synthesized_evidence: list[EvidenceSource] | None = None,
    ) -> AttributionResponse:
        return AttributionResponse(
            primaryMarket=prepared.context,
            moveSummary=prepared.move_summary,
            topCatalyst=prepared.top_catalyst,
            alternativeCatalysts=prepared.alternative_catalysts,
            confidence=prepared.confidence if confidence is None else confidence,
            dataQuality=prepared.data_quality,
            evidence=prepared.evidence,
            relatedMarkets=related_markets,
            synthesizedCatalyst=synthesized_catalyst,
            synthesizedEvidence=synthesized_evidence or [],
        )

    def _find_related_markets_safe(self, context: MarketClickContext) -> list[RelatedMarket]:
        step_start = time.perf_counter()
        try:
            result = self.related_markets.find_related_markets(context)
            logger.debug("[7/8] Found %d related markets in %.2fs", len(result), time.perf_counter() - step_start)
            return result
        except Exception:
            logger.exception("Continuing without related markets after related-market lookup failed.")
            return []

    def _run_synthesis_pipeline(
        self,
        context: MarketClickContext,
        move_summary: MoveSummary,
    ) -> tuple[SynthesizedCatalyst | None, list[EvidenceSource]]:
        step_start = time.perf_counter()
        try:
            logger.debug("[8a/8] Planning search queries (Gemini)")
            query_start = time.perf_counter()
            search_plan = self.catalyst_synthesis.plan_search_query(context)
            logger.debug(
                "[8a/8] Queries planned in %.2fs: primary='%s', alts=%d, type=%s",
                time.perf_counter() - query_start,
                search_plan.primary_query[:50],
                len(search_plan.alt_queries),
                search_plan.market_type,
            )

            logger.debug("[8b/8] Searching news (multi-query)")
            search_start = time.perf_counter()
            all_queries = search_plan.all_queries
            if len(all_queries) > 1:
                articles = self.news_search.search_multi_query(context, all_queries)
            else:
                articles = self.news_search.search_sync(context, search_query=search_plan.primary_query)
            logger.debug("[8b/8] Found %d articles in %.2fs", len(articles), time.perf_counter() - search_start)

            logger.debug("[8c/8] Ranking articles")
            rank_start = time.perf_counter()
            ranked_articles = self.catalyst_synthesis.rank_articles(context, articles)
            logger.debug("[8c/8] Ranked to %d articles in %.2fs", len(ranked_articles), time.perf_counter() - rank_start)

            logger.debug("[8d/8] Filtering and synthesizing catalyst (Gemini)")
            synth_start = time.perf_counter()
            synthesized_catalyst, relevant_articles = self.catalyst_synthesis.synthesize(
                context=context,
                articles=ranked_articles,
            )
            logger.debug("[8d/8] Synthesis completed in %.2fs", time.perf_counter() - synth_start)

            synthesized_evidence = self.catalyst_synthesis.articles_to_evidence(relevant_articles)
            logger.debug("[8/8] Full synthesis pipeline completed in %.2fs", time.perf_counter() - step_start)
            return (synthesized_catalyst, synthesized_evidence)
        except Exception:
            logger.exception("Continuing without synthesized catalyst after synthesis failed.")
            return (None, [])
