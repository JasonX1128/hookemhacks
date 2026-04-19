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
from backend.app.services.related_markets import PipelineData, RelatedMarketsService
from data_pipeline.market_state import MarketMetadataRecord, market_is_concluded, merge_market_records, prune_concluded_market_records


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
    assert 0.7 <= analysis.summary.jumpScore <= 0.85
    assert 0.4 <= analysis.data_quality <= 0.7


def test_move_analyzer_reconstructs_move_from_candles_when_click_context_is_sparse() -> None:
    class FakeKalshiClient:
        def fetch_market(self, market_id: str) -> dict[str, str]:
            assert market_id == "KXENERGY-WTI-MON"
            return {"event_ticker": "WTI-ON-MON"}

        def fetch_event(self, event_ticker: str) -> dict[str, str]:
            assert event_ticker == "WTI-ON-MON"
            return {"series_ticker": "WTI"}

        def fetch_candlesticks(self, *_: object, **__: object) -> list[dict[str, object]]:
            return [
                {
                    "end_period_ts": 1776518400,
                    "yes_bid": {"close_dollars": "0.50"},
                    "yes_ask": {"close_dollars": "0.52"},
                    "volume_fp": "120.00",
                },
                {
                    "end_period_ts": 1776518460,
                    "yes_bid": {"close_dollars": "0.51"},
                    "yes_ask": {"close_dollars": "0.53"},
                    "volume_fp": "118.00",
                },
                {
                    "end_period_ts": 1776518520,
                    "yes_bid": {"close_dollars": "0.52"},
                    "yes_ask": {"close_dollars": "0.54"},
                    "volume_fp": "122.00",
                },
                {
                    "end_period_ts": 1776518580,
                    "yes_bid": {"close_dollars": "0.53"},
                    "yes_ask": {"close_dollars": "0.55"},
                    "volume_fp": "125.00",
                },
                {
                    "end_period_ts": 1776518640,
                    "yes_bid": {"close_dollars": "0.54"},
                    "yes_ask": {"close_dollars": "0.56"},
                    "volume_fp": "130.00",
                },
                {
                    "end_period_ts": 1776518700,
                    "yes_bid": {"close_dollars": "0.61"},
                    "yes_ask": {"close_dollars": "0.63"},
                    "volume_fp": "420.00",
                },
            ]

    context = MarketClickContext(
        marketId="KXENERGY-WTI-MON",
        marketTitle="Oil Price (WTI) on Monday?",
        marketQuestion="Will WTI close higher on Monday?",
        clickedTimestamp="2026-04-18T13:25:00Z",
        clickedPrice=0.62,
        windowStart="2026-04-18T13:18:00Z",
        windowEnd="2026-04-18T13:27:00Z",
    )

    analysis = MoveAnalyzer(kalshi_client=FakeKalshiClient()).characterize_move(context)

    assert analysis.normalized_before == 0.51
    assert analysis.normalized_after == 0.62
    assert analysis.summary.moveDirection == "up"
    assert analysis.summary.moveMagnitude == 0.11
    assert analysis.summary.jumpScore > 0.7
    assert analysis.data_quality > 0.75


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
    assert any(
        market.status in {"possibly_lagging", "divergent"}
        or (market.note and "worth checking" in market.note.lower())
        for market in markets
    )
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


def test_market_merge_helpers_preserve_existing_open_markets_and_prune_concluded() -> None:
    existing = [
        MarketMetadataRecord(
            market_id="OPEN-OLD",
            ticker="OPEN-OLD",
            title="Existing open market",
            question="Existing open market?",
            status="active",
            close_time="2027-01-01T00:00:00Z",
        ),
        MarketMetadataRecord(
            market_id="CONCLUDED-OLD",
            ticker="CONCLUDED-OLD",
            title="Existing concluded market",
            question="Existing concluded market?",
            status="finalized",
            resolution_time="2026-04-18T00:00:00Z",
        ),
    ]
    incoming = [
        MarketMetadataRecord(
            market_id="OPEN-OLD",
            ticker="OPEN-OLD",
            title="Updated open market",
            question="Updated open market?",
            status="active",
            close_time="2027-02-01T00:00:00Z",
        ),
        MarketMetadataRecord(
            market_id="OPEN-NEW",
            ticker="OPEN-NEW",
            title="New open market",
            question="New open market?",
            status="initialized",
            close_time="2027-03-01T00:00:00Z",
        ),
    ]

    merged = merge_market_records(existing, incoming)
    assert [record.market_id for record in merged] == ["CONCLUDED-OLD", "OPEN-NEW", "OPEN-OLD"]
    assert merged[-1].title == "Updated open market"

    pruned = prune_concluded_market_records(merged)
    assert [record.market_id for record in pruned] == ["OPEN-NEW", "OPEN-OLD"]
    assert market_is_concluded(status="finalized") is True
    assert market_is_concluded(status="active", resolution_time="2027-01-01T00:00:00Z") is False


def test_metadata_only_related_markets_prefer_same_series_without_history() -> None:
    universe = [
        {
            "marketId": "NBA-LAKERS-OPENER-WIN",
            "title": "Will the Lakers win their opener?",
            "question": "Will the Lakers win the opening game of the season?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "lakers", "opener"],
            "eventTicker": "NBA-OPENING-NIGHT",
            "seriesTicker": "NBA-LAKERS-2026",
            "categoryScore": 0.38,
            "semanticBoost": 0.21,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "NBA-LAKERS-OPENER-MARGIN",
            "title": "Will the Lakers win by more than 5 points?",
            "question": "Will the Lakers beat their opening-night opponent by more than five points?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "lakers", "margin"],
            "eventTicker": "NBA-OPENING-NIGHT",
            "seriesTicker": "NBA-LAKERS-2026",
            "categoryScore": 0.41,
            "semanticBoost": 0.24,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "NFL-BEARS-WEEK1-WIN",
            "title": "Will the Bears win in Week 1?",
            "question": "Will Chicago win its first NFL game of the season?",
            "category": "sports",
            "families": ["sports", "football"],
            "tags": ["nfl", "bears", "week1"],
            "eventTicker": "NFL-WEEK1-OPEN",
            "seriesTicker": "NFL-BEARS-2026",
            "categoryScore": 0.35,
            "semanticBoost": 0.18,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
    ]
    context = MarketClickContext(
        marketId="NBA-LAKERS-OPENER-WIN",
        marketTitle="Will the Lakers win their opener?",
        marketQuestion="Will the Lakers win the opening game of the season?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
    )

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(context)

    assert markets
    assert markets[0].marketId == "NBA-LAKERS-OPENER-MARGIN"
    assert "same_event" in markets[0].relationTypes or "same_series" in markets[0].relationTypes


def test_metadata_only_related_markets_avoid_same_event_when_cross_event_options_exist() -> None:
    universe = [
        {
            "marketId": "NBA-LAKERS-OPENER-WIN",
            "title": "Will the Lakers win their opener?",
            "question": "Will the Lakers win the opening game of the season?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "lakers", "opener"],
            "eventTicker": "NBA-OPENING-NIGHT",
            "seriesTicker": "NBA-LAKERS-2026",
            "categoryScore": 0.38,
            "semanticBoost": 0.21,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "NBA-LAKERS-OPENER-MARGIN",
            "title": "Will the Lakers win by more than 5 points?",
            "question": "Will the Lakers beat their opening-night opponent by more than five points?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "lakers", "margin"],
            "eventTicker": "NBA-OPENING-NIGHT",
            "seriesTicker": "NBA-LAKERS-2026",
            "categoryScore": 0.41,
            "semanticBoost": 0.24,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "NBA-LAKERS-MAKE-PLAYOFFS",
            "title": "Will the Lakers make the playoffs in 2026?",
            "question": "Will the Lakers qualify for the NBA playoffs in 2026?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "lakers", "playoffs"],
            "eventTicker": "NBA-LAKERS-PLAYOFFS",
            "seriesTicker": "NBA-LAKERS-2026",
            "categoryScore": 0.43,
            "semanticBoost": 0.26,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
    ]
    context = MarketClickContext(
        marketId="NBA-LAKERS-OPENER-WIN",
        marketTitle="Will the Lakers win their opener?",
        marketQuestion="Will the Lakers win the opening game of the season?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
    )

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(context)

    assert markets
    assert [market.marketId for market in markets] == ["NBA-LAKERS-MAKE-PLAYOFFS"]
    assert all("same_event" not in market.relationTypes for market in markets)


def test_pipeline_metadata_resolves_event_style_market_ids_for_related_markets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = RelatedMarketsService(universe_override=None)
    pipeline_data = PipelineData(
        universe=[
            {
                "marketId": "KXBBCHARTPOSITIONSONG-26APR25SWI-1",
                "title": "Will SWIM be #1 on the Billboard Hot 100 during the week of Apr 25, 2026",
                "question": "If SWIM by BTS is ranked #1 on the Billboard Hot 100 chart for the Week of Apr 25, 2026, then the market resolves to Yes. 1",
                "families": ["kxbbchartpositionsong", "structured"],
                "tags": ["1", "binary", "active"],
                "eventTicker": "KXBBCHARTPOSITIONSONG-26APR25SWI",
                "seriesTicker": "KXBBCHARTPOSITIONSONG",
                "categoryScore": 1.0,
                "semanticBoost": 1.0,
                "historicalComovement": 0.15,
                "expectedReactionScore": 0.575,
                "residualZscore": 0.0,
                "enoughHistory": False,
            },
            {
                "marketId": "KXBBCHARTPOSITIONSONG-26APR25SWI-2",
                "title": "Will SWIM be #2 on the Billboard Hot 100 during the week of Apr 25, 2026",
                "question": "If SWIM by BTS is ranked #2 on the Billboard Hot 100 chart for the Week of Apr 25, 2026, then the market resolves to Yes. 2",
                "families": ["kxbbchartpositionsong", "structured"],
                "tags": ["2", "binary", "active"],
                "eventTicker": "KXBBCHARTPOSITIONSONG-26APR25SWI",
                "seriesTicker": "KXBBCHARTPOSITIONSONG",
                "categoryScore": 1.0,
                "semanticBoost": 1.0,
                "historicalComovement": 0.15,
                "expectedReactionScore": 0.575,
                "residualZscore": 0.0,
                "enoughHistory": False,
            },
        ],
        metadata_by_id={
            "KXBBCHARTPOSITIONSONG-26APR25SWI-1": {
                "market_id": "KXBBCHARTPOSITIONSONG-26APR25SWI-1",
                "title": "Will SWIM be #1 on the Billboard Hot 100 during the week of Apr 25, 2026",
                "question": "If SWIM by BTS is ranked #1 on the Billboard Hot 100 chart for the Week of Apr 25, 2026, then the market resolves to Yes. 1",
                "families": ["kxbbchartpositionsong", "structured"],
                "tags": ["1", "binary", "active"],
                "extra": {
                    "event_ticker": "KXBBCHARTPOSITIONSONG-26APR25SWI",
                    "series_ticker": "KXBBCHARTPOSITIONSONG",
                },
            },
            "KXBBCHARTPOSITIONSONG-26APR25SWI-2": {
                "market_id": "KXBBCHARTPOSITIONSONG-26APR25SWI-2",
                "title": "Will SWIM be #2 on the Billboard Hot 100 during the week of Apr 25, 2026",
                "question": "If SWIM by BTS is ranked #2 on the Billboard Hot 100 chart for the Week of Apr 25, 2026, then the market resolves to Yes. 2",
                "families": ["kxbbchartpositionsong", "structured"],
                "tags": ["2", "binary", "active"],
                "extra": {
                    "event_ticker": "KXBBCHARTPOSITIONSONG-26APR25SWI",
                    "series_ticker": "KXBBCHARTPOSITIONSONG",
                },
            },
        },
        pair_rows_by_market={},
        cointegration_by_pair={},
        signature=("test",),
    )
    context = MarketClickContext(
        marketId="kxbbchartpositionsong-26apr25swi",
        marketTitle="Where will 'SWIM' by BTS rank on the Billboard Hot 100 chart dated Apr 25, 2026?",
        marketQuestion="Where will 'SWIM' by BTS rank on the Billboard Hot 100 chart dated Apr 25, 2026?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
    )

    monkeypatch.setattr(service, "_load_pipeline_data", lambda: pipeline_data)

    markets = service.find_related_markets(context)

    assert markets
    assert {market.marketId for market in markets} <= {
        "KXBBCHARTPOSITIONSONG-26APR25SWI-1",
        "KXBBCHARTPOSITIONSONG-26APR25SWI-2",
    }
    assert any("same_event" in market.relationTypes for market in markets)

    path_style_context = MarketClickContext(
        marketId="markets:kxbbchartpositionsong:what-position-will-songalbum-be-on-the-billboard-chart:kxbbchartpositionsong-26apr25swi",
        marketTitle="Where will 'SWIM' by BTS rank on the Billboard Hot 100 chart dated Apr 25, 2026?",
        marketQuestion="Where will 'SWIM' by BTS rank on the Billboard Hot 100 chart dated Apr 25, 2026?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
    )

    path_style_markets = service.find_related_markets(path_style_context)

    assert path_style_markets
    assert {market.marketId for market in path_style_markets} <= {
        "KXBBCHARTPOSITIONSONG-26APR25SWI-1",
        "KXBBCHARTPOSITIONSONG-26APR25SWI-2",
    }
    assert any("same_event" in market.relationTypes for market in path_style_markets)


def test_pipeline_related_markets_avoid_same_event_when_cross_event_options_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = RelatedMarketsService(universe_override=None)
    pipeline_data = PipelineData(
        universe=[],
        metadata_by_id={
            "NBA-LAKERS-OPENER-WIN": {
                "market_id": "NBA-LAKERS-OPENER-WIN",
                "title": "Will the Lakers win their opener?",
                "question": "Will the Lakers win the opening game of the season?",
                "category": "sports",
                "families": ["sports", "basketball"],
                "tags": ["nba", "lakers", "opener"],
                "extra": {
                    "event_ticker": "NBA-OPENING-NIGHT",
                    "series_ticker": "NBA-LAKERS-2026",
                },
            },
            "NBA-LAKERS-OPENER-MARGIN": {
                "market_id": "NBA-LAKERS-OPENER-MARGIN",
                "title": "Will the Lakers win by more than 5 points?",
                "question": "Will the Lakers beat their opening-night opponent by more than five points?",
                "category": "sports",
                "families": ["sports", "basketball"],
                "tags": ["nba", "lakers", "margin"],
                "extra": {
                    "event_ticker": "NBA-OPENING-NIGHT",
                    "series_ticker": "NBA-LAKERS-2026",
                },
            },
            "NBA-LAKERS-MAKE-PLAYOFFS": {
                "market_id": "NBA-LAKERS-MAKE-PLAYOFFS",
                "title": "Will the Lakers make the playoffs in 2026?",
                "question": "Will the Lakers qualify for the NBA playoffs in 2026?",
                "category": "sports",
                "families": ["sports", "basketball"],
                "tags": ["nba", "lakers", "playoffs"],
                "extra": {
                    "event_ticker": "NBA-LAKERS-PLAYOFFS",
                    "series_ticker": "NBA-LAKERS-2026",
                },
            },
        },
        pair_rows_by_market={
            "NBA-LAKERS-OPENER-WIN": [
                {
                    "market_id": "NBA-LAKERS-OPENER-WIN",
                    "related_market_id": "NBA-LAKERS-OPENER-MARGIN",
                    "candidate_score": "0.92",
                    "semantic_similarity_score": "0.7",
                    "comovement_score": "0.82",
                    "return_correlation": "0.8",
                    "quick_return_correlation": "0.78",
                    "overlap_points": "48",
                },
                {
                    "market_id": "NBA-LAKERS-OPENER-WIN",
                    "related_market_id": "NBA-LAKERS-MAKE-PLAYOFFS",
                    "candidate_score": "0.74",
                    "semantic_similarity_score": "0.62",
                    "comovement_score": "0.66",
                    "return_correlation": "0.64",
                    "quick_return_correlation": "0.62",
                    "overlap_points": "48",
                },
            ]
        },
        cointegration_by_pair={},
        signature=("test",),
    )
    context = MarketClickContext(
        marketId="NBA-LAKERS-OPENER-WIN",
        marketTitle="Will the Lakers win their opener?",
        marketQuestion="Will the Lakers win the opening game of the season?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
    )

    monkeypatch.setattr(service, "_load_pipeline_data", lambda: pipeline_data)

    markets = service.find_related_markets(context)

    assert markets
    assert [market.marketId for market in markets] == ["NBA-LAKERS-MAKE-PLAYOFFS"]
    assert all("same_event" not in market.relationTypes for market in markets)


def test_metadata_only_related_markets_skip_concluded_candidates() -> None:
    universe = [
        {
            "marketId": "KXRATES-CONCLUDED-SEP2026",
            "title": "Will the Fed cut by September 2026?",
            "question": "Will the Federal Reserve cut rates by September 2026?",
            "categoryScore": 0.92,
            "semanticBoost": 0.34,
            "historicalComovement": 0.8,
            "expectedReactionScore": 0.76,
            "residualZscore": 0.2,
            "proxyType": "rates_proxy",
            "enoughHistory": True,
            "status": "finalized",
        },
        {
            "marketId": "KXRATES-OPEN-DEC2026",
            "title": "Will the Fed cut by December 2026?",
            "question": "Will the Federal Reserve cut rates by December 2026?",
            "categoryScore": 0.88,
            "semanticBoost": 0.3,
            "historicalComovement": 0.72,
            "expectedReactionScore": 0.7,
            "residualZscore": 0.15,
            "proxyType": "rates_proxy",
            "enoughHistory": True,
            "status": "active",
            "closeTime": "2026-12-18T19:00:00Z",
        },
    ]

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(build_context())

    assert markets
    assert [market.marketId for market in markets] == ["KXRATES-OPEN-DEC2026"]


def test_metadata_only_related_markets_backfill_low_quality_candidates_to_top_three() -> None:
    universe = [
        {
            "marketId": "KXINFLATION-CPI-MAY",
            "title": "Will CPI inflation be above 3% in May 2026?",
            "question": "Will CPI inflation print above 3% in May 2026?",
            "families": ["inflation"],
            "tags": ["cpi"],
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXGOLD-DEC",
            "title": "Will gold settle above $3500 in Dec 2026?",
            "question": "Will gold finish above 3500 in Dec 2026?",
            "families": ["gold"],
            "tags": ["gold"],
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXSPX-DEC",
            "title": "Will the S&P 500 finish above 7000 in Dec 2026?",
            "question": "Will SPX finish above 7000 by Dec 2026?",
            "families": ["equities"],
            "tags": ["spx", "stocks"],
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXJOBS-UNEMP-JUNE",
            "title": "Will unemployment rise in June 2026?",
            "question": "Will unemployment rise in June 2026?",
            "families": ["labor"],
            "tags": ["jobs", "unemployment"],
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
    ]
    context = MarketClickContext(
        marketId="KXINFLATION-CPI-MAY",
        marketTitle="Will CPI inflation be above 3% in May 2026?",
        marketQuestion="Will CPI inflation print above 3% in May 2026?",
        clickedTimestamp="2026-04-18T00:00:00Z",
        windowStart="2026-04-18T00:00:00Z",
        windowEnd="2026-04-18T01:00:00Z",
    )

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(context)

    assert [market.marketId for market in markets] == ["KXJOBS-UNEMP-JUNE", "KXGOLD-DEC", "KXSPX-DEC"]
    assert all("low_match_quality" in market.relationTypes for market in markets)
    assert all(market.status == "normal" for market in markets)
    assert all((market.note or "").startswith("Low match quality:") for market in markets)


def test_metadata_only_related_markets_prioritize_same_category_before_cross_category() -> None:
    universe = [
        {
            "marketId": "NBA-CELTICS-WIN-TONIGHT",
            "title": "Will the Celtics win tonight?",
            "question": "Will Boston win tonight's game?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "celtics"],
            "eventTicker": "NBA-TONIGHT-CELTICS",
            "seriesTicker": "NBA-CELTICS-2026",
            "categoryScore": 0.4,
            "semanticBoost": 0.22,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "NBA-CELTICS-MARGIN-TONIGHT",
            "title": "Will the Celtics win by more than 7 points tonight?",
            "question": "Will Boston win tonight by more than seven points?",
            "category": "sports",
            "families": ["sports", "basketball"],
            "tags": ["nba", "celtics", "margin"],
            "eventTicker": "NBA-TONIGHT-CELTICS",
            "seriesTicker": "NBA-CELTICS-2026",
            "categoryScore": 0.42,
            "semanticBoost": 0.25,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "BTC-ABOVE-130K-DEC2026",
            "title": "Will Bitcoin finish above 130k in December 2026?",
            "question": "Will Bitcoin finish above 130k in December 2026?",
            "category": "crypto",
            "families": ["crypto"],
            "tags": ["btc", "bitcoin"],
            "eventTicker": "BTC-DEC-2026",
            "seriesTicker": "BTC-2026",
            "categoryScore": 0.9,
            "semanticBoost": 0.55,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
    ]
    context = MarketClickContext(
        marketId="NBA-CELTICS-WIN-TONIGHT",
        marketTitle="Will the Celtics win tonight?",
        marketQuestion="Will Boston win tonight's game?",
        clickedTimestamp="2026-04-18T13:30:00Z",
        windowStart="2026-04-18T13:00:00Z",
        windowEnd="2026-04-18T14:00:00Z",
    )

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(context)

    assert markets
    assert markets[0].marketId == "NBA-CELTICS-MARGIN-TONIGHT"
    assert "same_category" in markets[0].relationTypes
    assert all(market.marketId != "BTC-ABOVE-130K-DEC2026" for market in markets[:1])


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


def test_pipeline_related_markets_skip_concluded_candidates_and_keep_searching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = RelatedMarketsService(universe_override=None)
    pipeline_data = PipelineData(
        universe=[],
        metadata_by_id={
            "KXINFLATION-CPI-MAY2026-ABOVE35": {
                "market_id": "KXINFLATION-CPI-MAY2026-ABOVE35",
                "title": "Will US CPI YoY print above 3.5% in May 2026?",
                "question": "Will the next CPI inflation print come in above 3.5% year-over-year?",
                "status": "active",
                "close_time": "2026-05-31T00:00:00Z",
            },
            "KXRATES-CONCLUDED-SEP2026": {
                "market_id": "KXRATES-CONCLUDED-SEP2026",
                "title": "Will the Fed cut by September 2026?",
                "question": "Will the Federal Reserve cut rates by September 2026?",
                "status": "finalized",
                "resolution_time": "2026-04-18T00:00:00Z",
            },
            "KXRATES-OPEN-DEC2026": {
                "market_id": "KXRATES-OPEN-DEC2026",
                "title": "Will the Fed cut by December 2026?",
                "question": "Will the Federal Reserve cut rates by December 2026?",
                "status": "active",
                "close_time": "2026-12-18T19:00:00Z",
            },
        },
        pair_rows_by_market={
            "KXINFLATION-CPI-MAY2026-ABOVE35": [
                {
                    "market_id": "KXINFLATION-CPI-MAY2026-ABOVE35",
                    "related_market_id": "KXRATES-CONCLUDED-SEP2026",
                    "candidate_score": "0.92",
                    "semantic_similarity_score": "0.68",
                    "comovement_score": "0.81",
                    "return_correlation": "0.79",
                    "quick_return_correlation": "0.77",
                    "overlap_points": "48",
                },
                {
                    "market_id": "KXINFLATION-CPI-MAY2026-ABOVE35",
                    "related_market_id": "KXRATES-OPEN-DEC2026",
                    "candidate_score": "0.86",
                    "semantic_similarity_score": "0.63",
                    "comovement_score": "0.74",
                    "return_correlation": "0.72",
                    "quick_return_correlation": "0.7",
                    "overlap_points": "48",
                },
            ]
        },
        cointegration_by_pair={},
        signature=("test",),
    )

    monkeypatch.setattr(service, "_load_pipeline_data", lambda: pipeline_data)

    markets = service.find_related_markets(build_context())

    assert markets
    assert [market.marketId for market in markets] == ["KXRATES-OPEN-DEC2026"]


def test_pipeline_related_markets_backfill_low_quality_candidates_to_top_three(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = RelatedMarketsService(universe_override=None)
    pipeline_data = PipelineData(
        universe=[],
        metadata_by_id={
            "KXINFLATION-CPI-MAY": {
                "market_id": "KXINFLATION-CPI-MAY",
                "title": "Will CPI inflation be above 3% in May 2026?",
                "question": "Will CPI inflation print above 3% in May 2026?",
                "families": ["inflation"],
                "tags": ["cpi"],
                "extra": {
                    "event_ticker": "KXINFLATION-CPI-MAY",
                    "series_ticker": "KXINFLATION",
                },
            },
            "KXGOLD-DEC": {
                "market_id": "KXGOLD-DEC",
                "title": "Will gold settle above $3500 in Dec 2026?",
                "question": "Will gold finish above 3500 in Dec 2026?",
                "families": ["gold"],
                "tags": ["gold"],
                "extra": {
                    "event_ticker": "KXGOLD-DEC",
                    "series_ticker": "KXGOLD",
                },
            },
            "KXSPX-DEC": {
                "market_id": "KXSPX-DEC",
                "title": "Will the S&P 500 finish above 7000 in Dec 2026?",
                "question": "Will SPX finish above 7000 by Dec 2026?",
                "families": ["equities"],
                "tags": ["spx", "stocks"],
                "extra": {
                    "event_ticker": "KXSPX-DEC",
                    "series_ticker": "KXSPX",
                },
            },
            "KXJOBS-UNEMP-JUNE": {
                "market_id": "KXJOBS-UNEMP-JUNE",
                "title": "Will unemployment rise in June 2026?",
                "question": "Will unemployment rise in June 2026?",
                "families": ["labor"],
                "tags": ["jobs", "unemployment"],
                "extra": {
                    "event_ticker": "KXJOBS-UNEMP-JUNE",
                    "series_ticker": "KXJOBS",
                },
            },
        },
        pair_rows_by_market={
            "KXINFLATION-CPI-MAY": [
                {
                    "market_id": "KXINFLATION-CPI-MAY",
                    "related_market_id": "KXJOBS-UNEMP-JUNE",
                    "candidate_score": "0.16",
                    "semantic_similarity_score": "0.18",
                    "comovement_score": "0.12",
                    "return_correlation": "0.10",
                    "quick_return_correlation": "0.08",
                    "overlap_points": "12",
                },
                {
                    "market_id": "KXINFLATION-CPI-MAY",
                    "related_market_id": "KXGOLD-DEC",
                    "candidate_score": "0.15",
                    "semantic_similarity_score": "0.16",
                    "comovement_score": "0.11",
                    "return_correlation": "0.09",
                    "quick_return_correlation": "0.08",
                    "overlap_points": "12",
                },
                {
                    "market_id": "KXINFLATION-CPI-MAY",
                    "related_market_id": "KXSPX-DEC",
                    "candidate_score": "0.14",
                    "semantic_similarity_score": "0.15",
                    "comovement_score": "0.10",
                    "return_correlation": "0.08",
                    "quick_return_correlation": "0.07",
                    "overlap_points": "12",
                },
            ]
        },
        cointegration_by_pair={},
        signature=("test",),
    )
    context = MarketClickContext(
        marketId="KXINFLATION-CPI-MAY",
        marketTitle="Will CPI inflation be above 3% in May 2026?",
        marketQuestion="Will CPI inflation print above 3% in May 2026?",
        clickedTimestamp="2026-04-18T00:00:00Z",
        windowStart="2026-04-18T00:00:00Z",
        windowEnd="2026-04-18T01:00:00Z",
    )

    monkeypatch.setattr(service, "_load_pipeline_data", lambda: pipeline_data)

    markets = service.find_related_markets(context)

    assert [market.marketId for market in markets] == ["KXJOBS-UNEMP-JUNE", "KXGOLD-DEC", "KXSPX-DEC"]
    assert all("low_match_quality" in market.relationTypes for market in markets)
    assert all(market.status == "normal" for market in markets)
    assert all((market.note or "").startswith("Low match quality:") for market in markets)


def test_pipeline_related_markets_resolve_event_style_ids_before_pair_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = RelatedMarketsService(universe_override=None)
    pipeline_data = PipelineData(
        universe=[],
        metadata_by_id={
            "KXWTI-26APR20-T88.99": {
                "market_id": "KXWTI-26APR20-T88.99",
                "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 20, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 20, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr20"],
                "extra": {
                    "event_ticker": "KXWTI-26APR20",
                    "series_ticker": "KXWTI",
                },
            },
            "KXWTI-26APR20-T80.99": {
                "market_id": "KXWTI-26APR20-T80.99",
                "title": "Will the WTI front-month settle oil price  be >80.99 on Apr 20, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 80.99 on Apr 20, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr20"],
                "extra": {
                    "event_ticker": "KXWTI-26APR20",
                    "series_ticker": "KXWTI",
                },
            },
            "KXWTI-26APR21-T88.99": {
                "market_id": "KXWTI-26APR21-T88.99",
                "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 21, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 21, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr21"],
                "extra": {
                    "event_ticker": "KXWTI-26APR21",
                    "series_ticker": "KXWTI",
                },
            },
        },
        pair_rows_by_market={
            "KXWTI-26APR20-T88.99": [
                {
                    "market_id": "KXWTI-26APR20-T88.99",
                    "related_market_id": "KXWTI-26APR20-T80.99",
                    "candidate_score": "0.91",
                    "semantic_similarity_score": "0.67",
                    "comovement_score": "0.81",
                    "return_correlation": "0.78",
                    "quick_return_correlation": "0.76",
                    "overlap_points": "48",
                },
                {
                    "market_id": "KXWTI-26APR20-T88.99",
                    "related_market_id": "KXWTI-26APR21-T88.99",
                    "candidate_score": "0.86",
                    "semantic_similarity_score": "0.7",
                    "comovement_score": "0.84",
                    "return_correlation": "0.8",
                    "quick_return_correlation": "0.79",
                    "overlap_points": "48",
                },
            ]
        },
        cointegration_by_pair={},
        signature=("test",),
    )
    context = MarketClickContext(
        marketId="kxwti-26apr20",
        marketTitle="Will the WTI front-month settle oil price  be >88.99 on Apr 20, 2026?",
        marketQuestion="Will the WTI front-month settle oil price be greater than 88.99 on Apr 20, 2026?",
        clickedTimestamp="2026-04-18T21:11:00Z",
        windowStart="2026-04-18T20:41:00Z",
        windowEnd="2026-04-18T21:41:00Z",
    )

    monkeypatch.setattr(service, "_load_pipeline_data", lambda: pipeline_data)

    markets = service.find_related_markets(context)

    assert markets
    assert [market.marketId for market in markets] == ["KXWTI-26APR21-T88.99"]
    assert "same_event" not in markets[0].relationTypes
    assert "same_series" in markets[0].relationTypes


def test_metadata_related_markets_prefer_non_siblings_within_shortlist() -> None:
    universe = [
        {
            "marketId": "KXWTI-26APR20-T88.99",
            "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 20, 2026?",
            "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 20, 2026?",
            "families": ["energy", "oil"],
            "tags": ["wti", "oil", "apr20"],
            "eventTicker": "KXWTI-26APR20",
            "seriesTicker": "KXWTI",
            "categoryScore": 0.4,
            "semanticBoost": 0.2,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.2,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXWTI-26APR21-T88.99",
            "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 21, 2026?",
            "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 21, 2026?",
            "families": ["energy", "oil"],
            "tags": ["wti", "oil", "apr21"],
            "eventTicker": "KXWTI-26APR21",
            "seriesTicker": "KXWTI",
            "categoryScore": 0.92,
            "semanticBoost": 0.45,
            "historicalComovement": 0.7,
            "expectedReactionScore": 0.76,
            "residualZscore": 0.0,
            "enoughHistory": True,
        },
        {
            "marketId": "KXWTI-26APR21-T90.99",
            "title": "Will the WTI front-month settle oil price  be >90.99 on Apr 21, 2026?",
            "question": "Will the WTI front-month settle oil price be greater than 90.99 on Apr 21, 2026?",
            "families": ["energy", "oil"],
            "tags": ["wti", "oil", "apr21"],
            "eventTicker": "KXWTI-26APR21",
            "seriesTicker": "KXWTI",
            "categoryScore": 0.89,
            "semanticBoost": 0.42,
            "historicalComovement": 0.68,
            "expectedReactionScore": 0.74,
            "residualZscore": 0.0,
            "enoughHistory": True,
        },
        {
            "marketId": "KXWTI-26APR22-T88.99",
            "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 22, 2026?",
            "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 22, 2026?",
            "families": ["energy", "oil"],
            "tags": ["wti", "oil", "apr22"],
            "eventTicker": "KXWTI-26APR22",
            "seriesTicker": "KXWTI",
            "categoryScore": 0.83,
            "semanticBoost": 0.36,
            "historicalComovement": 0.62,
            "expectedReactionScore": 0.69,
            "residualZscore": 0.0,
            "enoughHistory": True,
        },
    ]
    context = MarketClickContext(
        marketId="KXWTI-26APR20-T88.99",
        marketTitle="Will the WTI front-month settle oil price  be >88.99 on Apr 20, 2026?",
        marketQuestion="Will the WTI front-month settle oil price be greater than 88.99 on Apr 20, 2026?",
        clickedTimestamp="2026-04-18T21:11:00Z",
        windowStart="2026-04-18T20:41:00Z",
        windowEnd="2026-04-18T21:41:00Z",
    )

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(context)

    assert [market.marketId for market in markets[:3]] == [
        "KXWTI-26APR21-T88.99",
        "KXWTI-26APR22-T88.99",
        "KXWTI-26APR21-T90.99",
    ]


def test_metadata_related_markets_prefer_low_quality_non_siblings_before_extra_siblings() -> None:
    universe = [
        {
            "marketId": "KXHIGHLAX-26APR18-T80",
            "title": "Will the high temp in LA be >80° on Apr 18, 2026?",
            "question": "Will the high temp in LA be above 80° on Apr 18, 2026?",
            "families": ["weather"],
            "tags": ["high_temp", "la"],
            "eventTicker": "KXHIGHLAX-26APR18",
            "seriesTicker": "KXHIGHLAX",
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXHIGHLAX-26APR19-B68.5",
            "title": "Will the high temp in LA be 68-69° on Apr 19, 2026?",
            "question": "Will the high temp in LA be 68-69° on Apr 19, 2026?",
            "families": ["weather"],
            "tags": ["high_temp", "la"],
            "eventTicker": "KXHIGHLAX-26APR19",
            "seriesTicker": "KXHIGHLAX",
            "categoryScore": 1.0,
            "semanticBoost": 1.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.575,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXHIGHLAX-26APR19-T75",
            "title": "Will the high temp in LA be >75° on Apr 19, 2026?",
            "question": "Will the high temp in LA be above 75° on Apr 19, 2026?",
            "families": ["weather"],
            "tags": ["high_temp", "la"],
            "eventTicker": "KXHIGHLAX-26APR19",
            "seriesTicker": "KXHIGHLAX",
            "categoryScore": 1.0,
            "semanticBoost": 1.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.575,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXHIGHNY-26APR18-T68",
            "title": "Will the high temp in NYC be >68° on Apr 18, 2026?",
            "question": "Will the high temp in NYC be above 68° on Apr 18, 2026?",
            "families": ["weather"],
            "tags": ["high_temp", "nyc"],
            "eventTicker": "KXHIGHNY-26APR18",
            "seriesTicker": "KXHIGHNY",
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
        {
            "marketId": "KXHIGHCHI-26APR18-T70",
            "title": "Will the high temp in Chicago be >70° on Apr 18, 2026?",
            "question": "Will the high temp in Chicago be above 70° on Apr 18, 2026?",
            "families": ["weather"],
            "tags": ["high_temp", "chicago"],
            "eventTicker": "KXHIGHCHI-26APR18",
            "seriesTicker": "KXHIGHCHI",
            "categoryScore": 0.25,
            "semanticBoost": 0.0,
            "historicalComovement": 0.15,
            "expectedReactionScore": 0.18,
            "residualZscore": 0.0,
            "enoughHistory": False,
        },
    ]
    context = MarketClickContext(
        marketId="KXHIGHLAX-26APR18-T80",
        marketTitle="Will the high temp in LA be >80° on Apr 18, 2026?",
        marketQuestion="Will the high temp in LA be above 80° on Apr 18, 2026?",
        clickedTimestamp="2026-04-18T21:11:00Z",
        windowStart="2026-04-18T20:41:00Z",
        windowEnd="2026-04-18T21:41:00Z",
    )

    markets = RelatedMarketsService(universe_override=universe).find_related_markets(context)

    assert markets[0].marketId in {"KXHIGHLAX-26APR19-B68.5", "KXHIGHLAX-26APR19-T75"}
    assert {market.marketId for market in markets[1:3]} == {
        "KXHIGHNY-26APR18-T68",
        "KXHIGHCHI-26APR18-T70",
    }
    assert "low_match_quality" not in markets[0].relationTypes
    assert "low_match_quality" in markets[1].relationTypes
    assert "low_match_quality" in markets[2].relationTypes


def test_pipeline_related_markets_prefer_non_siblings_within_shortlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = RelatedMarketsService(universe_override=None)
    pipeline_data = PipelineData(
        universe=[],
        metadata_by_id={
            "KXWTI-26APR20-T88.99": {
                "market_id": "KXWTI-26APR20-T88.99",
                "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 20, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 20, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr20"],
                "extra": {
                    "event_ticker": "KXWTI-26APR20",
                    "series_ticker": "KXWTI",
                },
            },
            "KXWTI-26APR21-T88.99": {
                "market_id": "KXWTI-26APR21-T88.99",
                "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 21, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 21, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr21"],
                "extra": {
                    "event_ticker": "KXWTI-26APR21",
                    "series_ticker": "KXWTI",
                },
            },
            "KXWTI-26APR21-T90.99": {
                "market_id": "KXWTI-26APR21-T90.99",
                "title": "Will the WTI front-month settle oil price  be >90.99 on Apr 21, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 90.99 on Apr 21, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr21"],
                "extra": {
                    "event_ticker": "KXWTI-26APR21",
                    "series_ticker": "KXWTI",
                },
            },
            "KXWTI-26APR22-T88.99": {
                "market_id": "KXWTI-26APR22-T88.99",
                "title": "Will the WTI front-month settle oil price  be >88.99 on Apr 22, 2026?",
                "question": "Will the WTI front-month settle oil price be greater than 88.99 on Apr 22, 2026?",
                "families": ["energy", "oil"],
                "tags": ["wti", "oil", "apr22"],
                "extra": {
                    "event_ticker": "KXWTI-26APR22",
                    "series_ticker": "KXWTI",
                },
            },
        },
        pair_rows_by_market={
            "KXWTI-26APR20-T88.99": [
                {
                    "market_id": "KXWTI-26APR20-T88.99",
                    "related_market_id": "KXWTI-26APR21-T88.99",
                    "candidate_score": "0.92",
                    "semantic_similarity_score": "0.74",
                    "comovement_score": "0.85",
                    "return_correlation": "0.82",
                    "quick_return_correlation": "0.81",
                    "overlap_points": "48",
                },
                {
                    "market_id": "KXWTI-26APR20-T88.99",
                    "related_market_id": "KXWTI-26APR21-T90.99",
                    "candidate_score": "0.9",
                    "semantic_similarity_score": "0.72",
                    "comovement_score": "0.82",
                    "return_correlation": "0.79",
                    "quick_return_correlation": "0.78",
                    "overlap_points": "48",
                },
                {
                    "market_id": "KXWTI-26APR20-T88.99",
                    "related_market_id": "KXWTI-26APR22-T88.99",
                    "candidate_score": "0.84",
                    "semantic_similarity_score": "0.68",
                    "comovement_score": "0.74",
                    "return_correlation": "0.72",
                    "quick_return_correlation": "0.71",
                    "overlap_points": "48",
                },
            ]
        },
        cointegration_by_pair={},
        signature=("test",),
    )
    context = MarketClickContext(
        marketId="KXWTI-26APR20-T88.99",
        marketTitle="Will the WTI front-month settle oil price  be >88.99 on Apr 20, 2026?",
        marketQuestion="Will the WTI front-month settle oil price be greater than 88.99 on Apr 20, 2026?",
        clickedTimestamp="2026-04-18T21:11:00Z",
        windowStart="2026-04-18T20:41:00Z",
        windowEnd="2026-04-18T21:41:00Z",
    )

    monkeypatch.setattr(service, "_load_pipeline_data", lambda: pipeline_data)

    markets = service.find_related_markets(context)

    assert [market.marketId for market in markets[:3]] == [
        "KXWTI-26APR21-T88.99",
        "KXWTI-26APR22-T88.99",
        "KXWTI-26APR21-T90.99",
    ]


def test_attribution_service_returns_top_catalyst_and_related_markets() -> None:
    response = AttributionService().attribute_move(build_context())

    assert response.topCatalyst is not None
    assert response.topCatalyst.id == response.evidence[0].id
    assert len(response.alternativeCatalysts) >= 1
    assert len(response.evidence) >= 2
    assert response.relatedMarkets
    assert response.topCatalyst.totalScore is not None
    assert 0 < response.confidence < 0.9
    assert 0 <= response.dataQuality <= 1


def test_attribution_service_overview_returns_related_markets_before_ai_analysis() -> None:
    response = AttributionService().attribute_move_overview(build_context())

    assert response.topCatalyst is not None
    assert response.relatedMarkets
    assert response.synthesizedCatalyst is None
    assert response.synthesizedEvidence == []


def test_attribution_service_synthesis_returns_empty_payload_in_mock_mode() -> None:
    response = AttributionService().attribute_move_synthesis(build_context())

    assert response.synthesizedCatalyst is None
    assert response.synthesizedEvidence == []


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


def test_synthesis_prompts_are_compact_for_long_inputs() -> None:
    service = CatalystSynthesisService(project_id=None)
    long_title = "Will inflation stay hot after the next CPI release? " * 8
    long_question = "Does the market expect a hotter-than-expected CPI report and a hawkish Fed reaction? " * 10
    long_rules = "Resolve Yes if the official CPI print exceeds the threshold listed in the market. " * 12
    context = build_context().model_copy(
        update={
            "marketTitle": long_title,
            "marketQuestion": long_question,
            "marketRulesPrimary": long_rules,
        }
    )
    articles = [
        NewsArticle(
            title=f"Article {index} " + ("Fed and CPI headline " * 12),
            url=f"https://example.com/{index}",
            source="Example News Wire",
            snippet=("Stocks sold off after CPI came in hot and Treasury yields jumped. " * 18),
            date="2026-04-18T13:00:00Z",
        )
        for index in range(6)
    ]

    query_prompt = service._build_query_plan_prompt(context)
    filter_prompt = service._build_relevance_filter_prompt(
        context,
        "\n".join(
            service._format_article_prompt_block(index, article, include_snippet=False)
            for index, article in enumerate(articles[:5])
        ),
    )
    synthesis_prompt = service._build_synthesis_prompt(context, articles)

    assert len(query_prompt) < 900
    assert len(filter_prompt) < 1400
    assert len(synthesis_prompt) < 2200
    assert long_rules not in synthesis_prompt
    assert "..." in synthesis_prompt


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
