from __future__ import annotations

import pytest

from backend.app.models.contracts import CatalystCandidate, MarketClickContext, MoveSummary, RelatedMarket
from backend.app.services.attribution_service import AttributionService
from backend.app.services.catalyst_retrieval import CatalystRetrievalService
from backend.app.services.catalyst_scoring import CatalystScoringService
from backend.app.services.lagging_detector import annotate_market_status
from backend.app.services.move_analyzer import MoveAnalyzer
from backend.app.services.propagation import PropagationService
from backend.app.services.related_markets import RelatedMarketsService
from backend.app.services.utils import clamp_score, parse_timestamp, shift_timestamp, slugify, token_overlap, tokenize_text


def build_context() -> MarketClickContext:
    return MarketClickContext(
        marketId="KXINFLATION-CPI-MAY2026-ABOVE35",
        marketTitle="Will US CPI YoY print above 3.5% in May 2026?",
        marketQuestion="Will the next CPI inflation print come in above 3.5% year-over-year?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        clickedPrice=0.61,
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
        priceBefore=0.44,
        priceAfter=0.63,
    )


def test_utils_helpers_cover_clamping_timestamps_and_tokens() -> None:
    assert clamp_score(1.8) == 1.0
    assert clamp_score(-0.4) == 0.0
    assert clamp_score(0.66666, digits=3) == 0.667

    assert parse_timestamp("2026-04-18T13:30:00").isoformat() == "2026-04-18T13:30:00+00:00"
    assert shift_timestamp("2026-04-18T13:30:00Z", minutes=-15) == "2026-04-18T13:15:00Z"

    assert slugify("CPI Surprise!", "May 2026") == "cpi-surprise-may-2026"
    assert slugify("", "") == "market"

    tokens = tokenize_text("The CPI and rates moved up with this inflation surprise.")
    assert "the" not in tokens
    assert "and" not in tokens
    assert "cpi" in tokens
    assert "inflation" in tokens

    assert token_overlap("CPI inflation surprise", "Inflation preview for CPI print") > 0
    assert token_overlap("", "anything") == 0.0


def test_annotate_market_status_marks_divergent_for_large_residuals() -> None:
    market = RelatedMarket(
        marketId="KXRATES-FEDCUT-SEP2026",
        title="Will the Fed cut by September 2026?",
        relationTypes=["macro_overlap"],
        relationStrength=0.85,
        expectedReactionScore=0.25,
        residualZscore=3.2,
    )

    annotated = annotate_market_status(
        market,
        category_score=0.8,
        semantic_similarity=0.8,
        historical_comovement=0.8,
    )

    assert annotated.status == "divergent"
    assert annotated.note is not None and "moving unusually far" in annotated.note


def test_annotate_market_status_marks_lagging_for_muted_follow_through() -> None:
    market = RelatedMarket(
        marketId="KXGOLD-ABOVE3400-JUN2026",
        title="Will gold trade above $3,400 by June 2026?",
        relationTypes=["macro_overlap"],
        relationStrength=0.8,
        expectedReactionScore=0.9,
        residualZscore=1.6,
    )

    annotated = annotate_market_status(
        market,
        category_score=0.7,
        semantic_similarity=0.6,
        historical_comovement=0.65,
    )

    assert annotated.status == "possibly_lagging"
    assert annotated.note is not None and "follow-through still looks muted" in annotated.note


def test_annotate_market_status_defaults_to_normal_when_signals_are_small() -> None:
    market = RelatedMarket(
        marketId="KXSPX-ABOVE6200-JUN2026",
        title="Will SPX finish above 6200 by June 2026?",
        relationTypes=["cross_asset_proxy"],
        relationStrength=0.35,
        expectedReactionScore=0.2,
        residualZscore=0.5,
    )

    annotated = annotate_market_status(market)

    assert annotated.status == "normal"
    assert annotated.note is not None and "plausible macro linkage" in annotated.note


def test_propagation_service_build_move_summary_handles_thresholds_and_fallbacks() -> None:
    service = PropagationService()
    context = build_context().model_copy(
        update={"clickedPrice": 0.4, "priceBefore": None, "priceAfter": None},
    )
    down_context = build_context().model_copy(update={"priceBefore": 0.55, "priceAfter": 0.53})
    flat_context = build_context().model_copy(update={"priceBefore": 0.55, "priceAfter": 0.5401})

    fallback_summary = service.build_move_summary(context)
    down_summary = service.build_move_summary(down_context)
    flat_summary = service.build_move_summary(flat_context)

    assert fallback_summary.moveDirection == "up"
    assert fallback_summary.moveMagnitude == 0.08
    assert fallback_summary.jumpScore == 0.39
    assert down_summary.moveDirection == "down"
    assert flat_summary.moveDirection == "flat"


def test_propagation_service_propagates_expected_reaction_and_status() -> None:
    service = PropagationService()
    move_summary = MoveSummary(moveMagnitude=0.2, moveDirection="up", jumpScore=0.9)
    related_markets = [
        RelatedMarket(
            marketId="A",
            title="A",
            relationTypes=["macro_overlap"],
            relationStrength=0.2,
        ),
        RelatedMarket(
            marketId="B",
            title="B",
            relationTypes=["macro_overlap"],
            relationStrength=0.95,
        ),
        RelatedMarket(
            marketId="C",
            title="C",
            relationTypes=["macro_overlap"],
            relationStrength=0.1,
        ),
    ]

    propagated = service.propagate_to_related_markets(
        move_summary=move_summary,
        related_markets=related_markets,
    )

    assert propagated[0].status == "normal"
    assert propagated[1].status == "possibly_lagging"
    assert propagated[2].status == "divergent"
    assert propagated[1].note is not None and "upside follow-through" in propagated[1].note


def test_propagation_service_compute_confidence_with_and_without_top_catalyst() -> None:
    service = PropagationService()
    move_summary = MoveSummary(moveMagnitude=0.2, moveDirection="up", jumpScore=0.9)
    related_markets = [
        RelatedMarket(
            marketId="A",
            title="A",
            relationTypes=["macro_overlap"],
            relationStrength=0.8,
        ),
        RelatedMarket(
            marketId="B",
            title="B",
            relationTypes=["macro_overlap"],
            relationStrength=0.7,
        ),
    ]
    top_catalyst = CatalystCandidate(
        id="event-cpi-preview",
        type="scheduled_event",
        title="CPI preview",
        timestamp="2026-04-18T13:20:00Z",
        source="Fixture",
        totalScore=0.8,
    )

    with_top = service.compute_confidence(
        move_summary=move_summary,
        top_catalyst=top_catalyst,
        related_markets=related_markets,
    )
    without_top = service.compute_confidence(
        move_summary=MoveSummary(moveMagnitude=0.0, moveDirection="flat", jumpScore=0.5),
        top_catalyst=None,
        related_markets=[],
    )

    assert with_top == 0.82
    assert without_top == 0.46


def test_attribution_service_falls_back_when_move_analysis_and_scoring_steps_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = AttributionService()
    context = build_context()
    ranked_candidate = CatalystCandidate(
        id="event-cpi-preview",
        type="scheduled_event",
        title="CPI preview",
        timestamp="2026-04-18T13:20:00Z",
        source="Fixture",
        totalScore=0.73,
    )

    def raise_move_analyzer(*_: object, **__: object) -> object:
        raise RuntimeError("move analyzer unavailable")

    def fake_retrieve(*_: object, **__: object) -> list[object]:
        return []

    def fake_score(*_: object, **__: object) -> list[CatalystCandidate]:
        return [ranked_candidate]

    def raise_evidence(*_: object, **__: object) -> list[object]:
        raise RuntimeError("evidence unavailable")

    def raise_confidence(*_: object, **__: object) -> float:
        raise RuntimeError("confidence unavailable")

    monkeypatch.setattr(MoveAnalyzer, "characterize_move", raise_move_analyzer)
    monkeypatch.setattr(CatalystRetrievalService, "retrieve", fake_retrieve)
    monkeypatch.setattr(CatalystScoringService, "score", fake_score)
    monkeypatch.setattr(CatalystScoringService, "select_evidence", raise_evidence)
    monkeypatch.setattr(CatalystScoringService, "compute_confidence", raise_confidence)
    monkeypatch.setattr(RelatedMarketsService, "find_related_markets", lambda *args, **kwargs: [])

    response = service.attribute_move(context)

    assert response.moveSummary.moveDirection == "up"
    assert response.moveSummary.jumpScore == 0.79
    assert response.topCatalyst is not None
    assert response.topCatalyst.id == "event-cpi-preview"
    assert [candidate.id for candidate in response.evidence] == ["event-cpi-preview"]
    assert response.confidence == 0.451
