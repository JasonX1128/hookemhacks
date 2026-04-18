from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.app.models.contracts import MarketClickContext, RelatedMarket
from backend.app.models.scoring import RelatedMarketScoreBreakdown
from backend.app.services.cointegration import score_cointegration_pair
from backend.app.services.lagging_detector import annotate_market_status
from backend.app.storage.cache_repo import CacheRepository

STOPWORDS = {
    "the",
    "and",
    "for",
    "will",
    "with",
    "into",
    "that",
    "this",
    "near",
    "from",
    "have",
    "what",
    "when",
    "where",
    "next",
    "print",
    "come",
    "comes",
    "above",
    "below",
    "over",
    "under",
    "year",
    "years",
    "month",
    "months",
    "trade",
    "trading",
    "finish",
    "end",
    "before",
    "after",
    "through",
    "spot",
    "yoy",
    "qoq",
    "may",
    "june",
    "jun",
    "july",
    "jul",
    "aug",
    "sep",
    "sept",
    "oct",
    "nov",
    "dec",
    "jan",
    "feb",
    "mar",
    "apr",
    "usa",
}
TOKEN_ALIASES = {
    "cpi": "inflation",
    "inflation": "inflation",
    "inflationary": "inflation",
    "pce": "inflation",
    "fed": "rates",
    "federal": "rates",
    "reserve": "rates",
    "rate": "rates",
    "rates": "rates",
    "cut": "rates",
    "cuts": "rates",
    "hike": "rates",
    "hikes": "rates",
    "yield": "rates",
    "yields": "rates",
    "job": "labor",
    "jobs": "labor",
    "payroll": "labor",
    "payrolls": "labor",
    "employment": "labor",
    "unemployment": "labor",
    "equity": "equities",
    "equities": "equities",
    "stock": "equities",
    "stocks": "equities",
    "spx": "equities",
    "gold": "gold",
    "bitcoin": "btc",
    "crypto": "btc",
    "btc": "btc",
}
TOPIC_CLUSTER_MAP = {
    "inflation": "inflation",
    "rates": "rates",
    "labor": "labor",
    "equities": "equities",
    "gold": "metals",
    "btc": "crypto",
}
MARKET_ID_TOPIC_HINTS = {
    "KXINFLATION": {"inflation"},
    "KXRATES": {"rates"},
    "KXJOBS": {"labor"},
    "KXSPX": {"equities"},
    "KXGOLD": {"gold"},
    "KXBTC": {"btc"},
}
PROXY_TOPIC_HINTS = {
    "rates_proxy": {"rates"},
    "equity_proxy": {"equities"},
    "cross_asset_proxy": {"gold"},
    "btc_proxy": {"btc"},
}
CLUSTER_AFFINITY = {
    "inflation": {"inflation": 1.0, "rates": 0.88, "labor": 0.68, "equities": 0.55, "metals": 0.63, "crypto": 0.32},
    "rates": {"inflation": 0.88, "rates": 1.0, "labor": 0.72, "equities": 0.65, "metals": 0.56, "crypto": 0.42},
    "labor": {"inflation": 0.68, "rates": 0.72, "labor": 1.0, "equities": 0.58, "metals": 0.34, "crypto": 0.24},
    "equities": {"inflation": 0.55, "rates": 0.65, "labor": 0.58, "equities": 1.0, "metals": 0.37, "crypto": 0.48},
    "metals": {"inflation": 0.63, "rates": 0.56, "labor": 0.34, "equities": 0.37, "metals": 1.0, "crypto": 0.25},
    "crypto": {"inflation": 0.32, "rates": 0.42, "labor": 0.24, "equities": 0.48, "metals": 0.25, "crypto": 1.0},
}


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _canonicalize_token(token: str) -> str:
    return TOKEN_ALIASES.get(token, token)


def _tokenize(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        if len(token) <= 2 or token in STOPWORDS or token.isdigit():
            continue
        canonical = _canonicalize_token(token)
        if canonical not in STOPWORDS:
            tokens.add(canonical)
    return tokens


def _cluster_affinity(left_cluster: str, right_cluster: str) -> float:
    if left_cluster == right_cluster:
        return 1.0
    return CLUSTER_AFFINITY.get(left_cluster, {}).get(right_cluster, 0.0)


def _soft_set_similarity(left: set[str], right: set[str], *, cluster_mode: bool = False) -> float:
    if not left or not right:
        return 0.0

    def pair_affinity(left_value: str, right_value: str) -> float:
        if left_value == right_value:
            return 1.0
        if cluster_mode:
            return _cluster_affinity(left_value, right_value)
        left_cluster = TOPIC_CLUSTER_MAP.get(left_value, left_value)
        right_cluster = TOPIC_CLUSTER_MAP.get(right_value, right_value)
        return _cluster_affinity(left_cluster, right_cluster) * 0.92

    left_best = [max(pair_affinity(left_value, right_value) for right_value in right) for left_value in left]
    right_best = [max(pair_affinity(right_value, left_value) for left_value in left) for right_value in right]
    return round(_clamp((sum(left_best) / len(left_best) + sum(right_best) / len(right_best)) / 2), 4)


def _token_overlap_score(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    overlap = len(left & right)
    return round(_clamp(overlap / max(1, min(len(left), len(right)))), 4)


def _market_family(market_id: str, clusters: set[str]) -> str:
    market_prefix = market_id.split("-", 1)[0].upper()
    if market_prefix in MARKET_ID_TOPIC_HINTS:
        hinted_topics = MARKET_ID_TOPIC_HINTS[market_prefix]
        for topic in hinted_topics:
            return TOPIC_CLUSTER_MAP.get(topic, topic)
    if clusters:
        return sorted(clusters)[0]
    return market_prefix.lower()


@dataclass(slots=True)
class MarketProfile:
    market_id: str
    tokens: set[str]
    topics: set[str]
    clusters: set[str]
    family: str


@dataclass(slots=True)
class IndexedUniverse:
    markets: dict[str, dict[str, Any]]
    profiles: dict[str, MarketProfile]
    topic_index: dict[str, set[str]]
    cluster_index: dict[str, set[str]]
    family_index: dict[str, set[str]]


@dataclass(slots=True)
class CandidateSeed:
    market_id: str
    generation_score: float


def _build_market_profile(
    *,
    market_id: str,
    title: str,
    question: str,
    proxy_type: str | None = None,
) -> MarketProfile:
    tokens = _tokenize(f"{title} {question}")
    topics = {token for token in tokens if token in TOPIC_CLUSTER_MAP}

    market_prefix = market_id.split("-", 1)[0].upper()
    topics.update(MARKET_ID_TOPIC_HINTS.get(market_prefix, set()))
    if proxy_type is not None:
        topics.update(PROXY_TOPIC_HINTS.get(proxy_type, set()))

    clusters = {TOPIC_CLUSTER_MAP[topic] for topic in topics if topic in TOPIC_CLUSTER_MAP}
    return MarketProfile(
        market_id=market_id,
        tokens=tokens,
        topics=topics,
        clusters=clusters,
        family=_market_family(market_id, clusters),
    )


def _dedupe_preserving_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


class RelatedMarketsService:
    def __init__(
        self,
        cache_repo: CacheRepository | None = None,
        *,
        universe_override: list[dict[str, Any]] | None = None,
    ) -> None:
        self.cache_repo = cache_repo or CacheRepository()
        self.universe_override = universe_override
        self.fixture_path = Path(__file__).resolve().parents[3] / "data_pipeline" / "fixtures" / "macro_market_universe.json"
        self._indexed_universe: IndexedUniverse | None = None

    def _load_fixture(self) -> list[dict[str, Any]]:
        if self.universe_override is not None:
            return self.universe_override

        cached = self.cache_repo.get_json("fixture", "macro_market_universe", max_age_seconds=60)
        if cached is None:
            with self.fixture_path.open("r", encoding="utf-8") as fixture_file:
                cached = json.load(fixture_file)
            self.cache_repo.set_json("fixture", "macro_market_universe", cached)
        return cached

    def _indexed_fixture(self) -> IndexedUniverse:
        if self._indexed_universe is not None:
            return self._indexed_universe

        markets: dict[str, dict[str, Any]] = {}
        profiles: dict[str, MarketProfile] = {}
        topic_index: dict[str, set[str]] = defaultdict(set)
        cluster_index: dict[str, set[str]] = defaultdict(set)
        family_index: dict[str, set[str]] = defaultdict(set)

        for candidate in self._load_fixture():
            market_id = str(candidate.get("marketId", "")).strip()
            title = str(candidate.get("title", "")).strip()
            if not market_id or not title:
                continue

            profile = _build_market_profile(
                market_id=market_id,
                title=title,
                question=str(candidate.get("question", "")),
                proxy_type=str(candidate.get("proxyType")) if candidate.get("proxyType") else None,
            )
            markets[market_id] = candidate
            profiles[market_id] = profile
            family_index[profile.family].add(market_id)
            for topic in profile.topics:
                topic_index[topic].add(market_id)
            for cluster in profile.clusters:
                cluster_index[cluster].add(market_id)

        self._indexed_universe = IndexedUniverse(
            markets=markets,
            profiles=profiles,
            topic_index=dict(topic_index),
            cluster_index=dict(cluster_index),
            family_index=dict(family_index),
        )
        return self._indexed_universe

    def _primary_profile(self, context: MarketClickContext) -> MarketProfile:
        return _build_market_profile(
            market_id=context.marketId,
            title=context.marketTitle,
            question=context.marketQuestion,
        )

    def _generate_candidate_seeds(
        self,
        primary_profile: MarketProfile,
        indexed_universe: IndexedUniverse,
    ) -> list[CandidateSeed]:
        candidate_weights: dict[str, float] = {}

        def add_candidate(market_id: str, weight: float) -> None:
            if market_id == primary_profile.market_id:
                return
            candidate_weights[market_id] = max(candidate_weights.get(market_id, 0.0), weight)

        for topic in primary_profile.topics:
            for market_id in indexed_universe.topic_index.get(topic, set()):
                add_candidate(market_id, 0.95)

        for cluster in primary_profile.clusters:
            for market_id in indexed_universe.cluster_index.get(cluster, set()):
                add_candidate(market_id, 0.85)
            for related_cluster, affinity in CLUSTER_AFFINITY.get(cluster, {}).items():
                if affinity < 0.55:
                    continue
                for market_id in indexed_universe.cluster_index.get(related_cluster, set()):
                    add_candidate(market_id, 0.3 + 0.45 * affinity)

        for market_id in indexed_universe.family_index.get(primary_profile.family, set()):
            add_candidate(market_id, 0.9)

        ranked_candidates = sorted(
            candidate_weights.items(),
            key=lambda item: (
                item[1],
                float(indexed_universe.markets[item[0]].get("categoryScore", 0.0)),
                float(indexed_universe.markets[item[0]].get("historicalComovement", 0.0)),
            ),
            reverse=True,
        )

        seeds: list[CandidateSeed] = []
        for market_id, generation_score in ranked_candidates:
            candidate_profile = indexed_universe.profiles[market_id]
            topic_similarity = _soft_set_similarity(primary_profile.topics, candidate_profile.topics)
            category_similarity = _soft_set_similarity(
                primary_profile.clusters,
                candidate_profile.clusters,
                cluster_mode=True,
            )
            if max(generation_score, topic_similarity, category_similarity) < 0.42:
                continue
            seeds.append(
                CandidateSeed(
                    market_id=market_id,
                    generation_score=round(
                        _clamp(max(generation_score, 0.55 * topic_similarity + 0.45 * category_similarity)),
                        4,
                    ),
                )
            )

        return seeds[:12]

    def _score_candidate(
        self,
        primary_profile: MarketProfile,
        candidate: dict[str, Any],
        candidate_profile: MarketProfile,
        *,
        generation_score: float,
    ) -> RelatedMarket | None:
        category_prior = float(candidate.get("categoryScore", 0.35))
        semantic_prior = float(candidate.get("semanticBoost", 0.0))
        historical_prior = float(candidate.get("historicalComovement", 0.35))

        topic_score = _soft_set_similarity(primary_profile.topics, candidate_profile.topics)
        category_similarity = _soft_set_similarity(
            primary_profile.clusters,
            candidate_profile.clusters,
            cluster_mode=True,
        )
        category_score = round(
            _clamp(0.5 * category_similarity + 0.25 * category_prior + 0.25 * generation_score),
            4,
        )
        lexical_overlap = _token_overlap_score(primary_profile.tokens, candidate_profile.tokens)
        semantic_similarity = round(
            _clamp(
                0.25 * lexical_overlap
                + 0.45 * topic_score
                + 0.15 * generation_score
                + 0.15 * _clamp(semantic_prior + 0.2)
            ),
            4,
        )
        historical_comovement = round(
            _clamp(0.7 * historical_prior + 0.15 * category_score + 0.15 * topic_score),
            4,
        )
        cointegration_bonus = score_cointegration_pair(
            primary_profile.market_id,
            candidate_profile.market_id,
            enough_history=bool(candidate.get("enoughHistory", False)),
            category_score=category_score,
            topic_score=topic_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            primary_topics=primary_profile.topics,
            related_topics=candidate_profile.topics,
        )
        score = RelatedMarketScoreBreakdown(
            category_score=category_score,
            topic_score=topic_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            cointegration_bonus=cointegration_bonus,
        )
        if score.total < 0.45:
            return None

        relation_types: list[str] = []
        if category_score >= 0.6:
            relation_types.append("macro_cluster")
        if topic_score >= 0.55:
            relation_types.append("topic_match")
        if semantic_similarity >= 0.5:
            relation_types.append("semantic_similarity")
        if historical_comovement >= 0.55:
            relation_types.append("historical_comovement")
        if cointegration_bonus > 0:
            relation_types.append("cointegration_signal")
        if candidate.get("proxyType"):
            relation_types.append(str(candidate["proxyType"]))

        return annotate_market_status(
            RelatedMarket(
                marketId=candidate_profile.market_id,
                title=str(candidate.get("title", candidate_profile.market_id)),
                relationTypes=_dedupe_preserving_order(relation_types or ["semantic_similarity"]),
                relationStrength=score.total,
                expectedReactionScore=float(candidate.get("expectedReactionScore", historical_comovement)),
                residualZscore=float(candidate.get("residualZscore", 0.0)),
                note=str(candidate.get("note")) if candidate.get("note") else None,
            ),
            category_score=category_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            cointegration_bonus=cointegration_bonus,
        )

    def find_related_markets(self, context: MarketClickContext) -> list[RelatedMarket]:
        indexed_universe = self._indexed_fixture()
        primary_profile = self._primary_profile(context)
        related: list[RelatedMarket] = []

        for seed in self._generate_candidate_seeds(primary_profile, indexed_universe):
            candidate = indexed_universe.markets[seed.market_id]
            related_market = self._score_candidate(
                primary_profile,
                candidate,
                indexed_universe.profiles[seed.market_id],
                generation_score=seed.generation_score,
            )
            if related_market is not None:
                related.append(related_market)

        related.sort(key=lambda item: item.relationStrength, reverse=True)
        return related[:5]
