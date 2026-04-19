from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

os.environ["BACKEND_MOCK_MODE"] = "True"

from backend.app.core.config import get_settings

get_settings.cache_clear()

from backend.app.models.contracts import MarketClickContext
from backend.app.services.attribution_service import AttributionService
from backend.app.services.catalyst_retrieval import CatalystRetrievalService
from backend.app.services.catalyst_scoring import CatalystScoringService
from backend.app.services.catalyst_synthesis import CatalystSynthesisService
from backend.app.services.cointegration import score_cointegration_pair
from backend.app.services.market_context import MarketContextService
from backend.app.services.move_analyzer import MoveAnalyzer
from backend.app.services.news_search import NewsArticle, NewsSearchService
from backend.app.services.related_markets import RelatedMarketsService


def build_context() -> MarketClickContext:
    return MarketClickContext(
        marketId="KXINFLATION-CPI-MAY2026-ABOVE35",
        marketTitle="Will US CPI YoY print above 3.5% in May 2026?",
        marketQuestion="Will the next CPI inflation print come in above 3.5% year-over-year?",
        marketSubtitle="May 2026 CPI year-over-year above 3.5%",
        marketRulesPrimary="Resolves Yes if the official May 2026 CPI print is above 3.5% year-over-year.",
        clickedTimestamp="2026-04-18T13:30:00Z",
        clickedPrice=0.61,
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
        priceBefore=0.44,
        priceAfter=0.63,
    )


def test_move_analyzer_characterizes_up_move() -> None:
    analysis = MoveAnalyzer().characterize_move(build_context())

    assert analysis.summary.moveDirection == "up"
    assert analysis.summary.moveMagnitude == 0.19
    assert analysis.summary.jumpScore == 0.95


def test_retrieval_service_combines_stub_sources() -> None:
    context = build_context()
    move_summary = MoveAnalyzer().characterize_move(context).summary
    candidates = CatalystRetrievalService().retrieve(context, move_summary)

    candidate_types = {candidate.type for candidate in candidates}
    candidate_ids = {candidate.id for candidate in candidates}

    assert {"headline", "scheduled_event", "platform_signal"} <= candidate_types
    assert "headline-cpi-preview-1" in candidate_ids
    assert "event-fed-minutes" in candidate_ids


def test_scoring_prefers_more_relevant_candidate() -> None:
    context = build_context()
    move_summary = MoveAnalyzer().characterize_move(context).summary
    ranked = CatalystScoringService().score(
        context=context,
        move_summary=move_summary,
        candidates=CatalystRetrievalService().retrieve(context, move_summary),
    )

    assert ranked[0].id in {"headline-cpi-preview-1", "event-cpi-preview", "event-fed-minutes"}
    assert ranked[0].type in {"headline", "scheduled_event"}
    assert ranked[0].timeScore is not None
    assert ranked[0].semanticScore is not None
    assert ranked[0].importanceScore is not None
    assert (ranked[0].totalScore or 0) > (ranked[-1].totalScore or 0)


def test_compat_ranker_prefers_more_relevant_candidate() -> None:
    context = build_context()
    from backend.app.services.catalyst_ranker import rank_candidates

    ranked = rank_candidates(
        context,
        MoveAnalyzer().characterize_move(context).summary,
        [
            {
                "id": "weaker",
                "type": "headline",
                "title": "Crypto narrative drifted sideways",
                "timestamp": "2026-04-18T10:00:00Z",
                "source": "Fixture",
                "importance": 0.3,
                "keywords": ["crypto"],
            },
            {
                "id": "stronger",
                "type": "scheduled_event",
                "title": "Sticky CPI preview and hawkish Fed chatter",
                "timestamp": "2026-04-18T13:25:00Z",
                "source": "Fixture",
                "importance": 0.8,
                "keywords": ["cpi", "inflation", "fed", "rates"],
            },
        ],
    )

    assert ranked[0].id == "stronger"
    assert (ranked[0].totalScore or 0) > (ranked[1].totalScore or 0)


def test_related_markets_include_worth_checking_signal() -> None:
    markets = RelatedMarketsService().find_related_markets(build_context())

    assert markets
    assert any(market.status == "possibly_lagging" for market in markets)
    assert all(market.status in {"normal", "possibly_lagging", "divergent"} for market in markets)


def test_related_markets_shortlist_candidates_without_scoring_unrelated_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    universe = [
        {
            "marketId": "KXRATES-FEDCUT-SEP2026",
            "title": "Will the Fed cut by September 2026?",
            "question": "Will the Federal Reserve cut rates by September 2026?",
            "categoryScore": 0.88,
            "semanticBoost": 0.34,
            "historicalComovement": 0.79,
            "expectedReactionScore": 0.76,
            "residualZscore": 0.4,
            "proxyType": "rates_proxy",
            "enoughHistory": True,
        },
        {
            "marketId": "KXSPORTS-LAKERS-2026",
            "title": "Will the Lakers win their opener?",
            "question": "Will Los Angeles win the first game of the season?",
            "categoryScore": 0.91,
            "semanticBoost": 0.1,
            "historicalComovement": 0.9,
            "expectedReactionScore": 0.8,
            "residualZscore": 0.2,
            "enoughHistory": True,
        },
    ]
    called_pairs: list[tuple[str, str]] = []

    def fake_cointegration(primary_market_id: str, related_market_id: str, **_: object) -> float:
        called_pairs.append((primary_market_id, related_market_id))
        return 0.05

    monkeypatch.setattr("backend.app.services.related_markets.score_cointegration_pair", fake_cointegration)

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(build_context())

    assert [market.marketId for market in markets] == ["KXRATES-FEDCUT-SEP2026"]
    assert called_pairs == [("KXINFLATION-CPI-MAY2026-ABOVE35", "KXRATES-FEDCUT-SEP2026")]


def test_cointegration_helper_only_returns_bonus_for_plausible_pairs() -> None:
    strong_pair = score_cointegration_pair(
        "KXINFLATION-CPI-MAY2026-ABOVE35",
        "KXRATES-FEDCUT-SEP2026",
        enough_history=True,
        category_score=0.84,
        topic_score=0.81,
        semantic_similarity=0.64,
        historical_comovement=0.83,
        primary_topics={"inflation"},
        related_topics={"rates"},
    )
    weak_pair = score_cointegration_pair(
        "KXINFLATION-CPI-MAY2026-ABOVE35",
        "KXBTC-ABOVE120K-JUN2026",
        enough_history=True,
        category_score=0.39,
        topic_score=0.28,
        semantic_similarity=0.22,
        historical_comovement=0.41,
        primary_topics={"inflation"},
        related_topics={"btc"},
    )
    no_history = score_cointegration_pair(
        "KXINFLATION-CPI-MAY2026-ABOVE35",
        "KXRATES-FEDCUT-SEP2026",
        enough_history=False,
        category_score=0.84,
        topic_score=0.81,
        semantic_similarity=0.64,
        historical_comovement=0.83,
        primary_topics={"inflation"},
        related_topics={"rates"},
    )

    assert strong_pair > 0
    assert weak_pair == 0.0
    assert no_history == 0.0


def test_related_market_status_can_surface_divergence() -> None:
    universe = [
        {
            "marketId": "KXRATES-FEDCUT-SEP2026",
            "title": "Will the Fed cut by September 2026?",
            "question": "Will the Federal Reserve cut rates by September 2026?",
            "categoryScore": 0.88,
            "semanticBoost": 0.34,
            "historicalComovement": 0.79,
            "expectedReactionScore": 0.32,
            "residualZscore": 3.1,
            "proxyType": "rates_proxy",
            "enoughHistory": True,
        },
        {
            "marketId": "KXGOLD-ABOVE3400-JUN2026",
            "title": "Will gold trade above $3,400 by June 2026?",
            "question": "Will spot gold trade above $3,400 before the June contract expires?",
            "categoryScore": 0.61,
            "semanticBoost": 0.18,
            "historicalComovement": 0.51,
            "expectedReactionScore": 0.58,
            "residualZscore": 2.1,
            "proxyType": "cross_asset_proxy",
        },
    ]

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(build_context())
    statuses = {market.marketId: market.status for market in markets}

    assert statuses["KXRATES-FEDCUT-SEP2026"] == "divergent"
    assert statuses["KXGOLD-ABOVE3400-JUN2026"] == "possibly_lagging"


def test_attribution_service_returns_top_catalyst_and_related_markets() -> None:
    response = AttributionService().attribute_move(build_context())

    assert response.topCatalyst is not None
    assert response.topCatalyst.id == response.evidence[0].id
    assert len(response.alternativeCatalysts) >= 1
    assert len(response.evidence) >= 2
    assert response.relatedMarkets
    assert response.topCatalyst.totalScore is not None
    assert 0 < response.confidence < 0.9


def test_attribution_service_fails_gracefully_when_optional_components_break(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = AttributionService()

    def raise_retrieval_error(*_: object, **__: object) -> list[object]:
        raise RuntimeError("retrieval unavailable")

    def raise_related_market_error(*_: object, **__: object) -> list[object]:
        raise RuntimeError("related markets unavailable")

    monkeypatch.setattr(CatalystRetrievalService, "retrieve", raise_retrieval_error)
    monkeypatch.setattr(RelatedMarketsService, "find_related_markets", raise_related_market_error)

    response = service.attribute_move(build_context())

    assert response.primaryMarket.marketId == "KXINFLATION-CPI-MAY2026-ABOVE35"
    assert response.moveSummary.moveDirection == "up"
    assert response.topCatalyst is None
    assert response.alternativeCatalysts == []
    assert response.evidence == []
    assert response.relatedMarkets == []
    assert response.confidence >= 0


def test_market_context_service_prefers_authoritative_kalshi_metadata() -> None:
    class StubKalshiClient:
        def fetch_market(self, market_id: str) -> dict[str, str]:
            assert market_id == "KXINFLATION-CPI-MAY2026-ABOVE35"
            return {
                "ticker": market_id,
                "title": "Will CPI print above 3.5% in May 2026?",
                "subtitle": "May 2026 CPI above 3.5%",
                "rules_primary": "Resolves Yes if the official CPI release is above 3.5% YoY.",
            }

    context = build_context().model_copy(update={"marketSubtitle": None, "marketRulesPrimary": None})
    hydrated = MarketContextService(kalshi_client=StubKalshiClient()).hydrate_context(context)

    assert hydrated.marketTitle == "Will CPI print above 3.5% in May 2026?"
    assert hydrated.marketSubtitle == "May 2026 CPI above 3.5%"
    assert hydrated.marketRulesPrimary == "Resolves Yes if the official CPI release is above 3.5% YoY."
    assert hydrated.marketQuestion == "May 2026 CPI above 3.5%"


def test_news_search_uses_specific_date_range_for_historical_clicks() -> None:
    service = NewsSearchService(api_key="test-key")

    tbs = service.build_time_filter(
        build_context(),
        now=datetime(2026, 4, 25, 12, 0, tzinfo=UTC),
    )

    assert tbs == "cdr:1,cd_min:04/18/2026,cd_max:04/19/2026"


def test_news_search_uses_recent_filter_for_fresh_clicks() -> None:
    service = NewsSearchService(api_key="test-key")
    context = build_context().model_copy(
        update={
            "clickedTimestamp": "2026-04-18T13:30:00Z",
            "windowStart": "2026-04-18T13:00:00Z",
            "windowEnd": "2026-04-18T14:00:00Z",
        }
    )

    tbs = service.build_time_filter(
        context,
        now=datetime(2026, 4, 19, 8, 0, tzinfo=UTC),
    )

    assert tbs == "qdr:d"


def test_article_ranking_prefers_articles_that_match_market_entities() -> None:
    service = CatalystSynthesisService(project_id=None)
    articles = [
        NewsArticle(
            title="Sticky CPI data raises odds of a hawkish Fed path",
            url="https://example.com/cpi",
            source="Example News",
            snippet="Economists now expect inflation to stay above target after the next CPI release.",
        ),
        NewsArticle(
            title="Bitcoin traders brace for weekend volatility",
            url="https://example.com/btc",
            source="Example News",
            snippet="Crypto markets sold off ahead of a major options expiry.",
        ),
    ]

    ranked = service.rank_articles(build_context(), articles)

    assert ranked[0].title == "Sticky CPI data raises odds of a hawkish Fed path"
    assert (ranked[0].relevanceScore or 0.0) > (ranked[1].relevanceScore or 0.0)
    assert (ranked[0].alignmentScore or 0.0) > (ranked[1].alignmentScore or 0.0)


def test_model_json_call_retries_after_truncated_response() -> None:
    class FakeCandidate:
        def __init__(self, finish_reason: str = "STOP", finish_message: str = "") -> None:
            self.finish_reason = finish_reason
            self.finish_message = finish_message

    class FakeResponse:
        def __init__(self, text: str, finish_reason: str = "STOP", finish_message: str = "") -> None:
            self.text = text
            self.candidates = [FakeCandidate(finish_reason=finish_reason, finish_message=finish_message)]

        def to_dict(self) -> dict[str, object]:
            return {
                "candidates": [
                    {
                        "finishReason": "STOP",
                        "content": {"parts": [{"text": self.text}]},
                    }
                ]
            }

    class FakeModel:
        def __init__(self) -> None:
            self.calls = 0

        def generate_content(self, *_: object, **__: object) -> FakeResponse:
            self.calls += 1
            if self.calls == 1:
                return FakeResponse('{"analysis":"Sticky CPI surpr')
            return FakeResponse('{"analysis":"Sticky CPI surprise drove the move.","relevant_indices":[0],"used_market_rules":false}')

    service = CatalystSynthesisService(project_id=None)
    payload = service._call_model_json(
        model=FakeModel(),
        prompt="Return JSON",
        schema={"type": "object"},
        temperature=0.2,
        max_output_tokens=128,
        model_name="fake-model",
    )

    assert payload == {
        "analysis": "Sticky CPI surprise drove the move.",
        "relevant_indices": [0],
        "used_market_rules": False,
    }


def test_best_effort_json_parse_can_close_missing_brace() -> None:
    service = CatalystSynthesisService(project_id=None)

    payload = service._best_effort_json_parse(
        '{"analysis":"Rates repriced after CPI.","relevant_indices":[0],"used_market_rules":false'
    )

    assert payload == {
        "analysis": "Rates repriced after CPI.",
        "relevant_indices": [0],
        "used_market_rules": False,
    }
