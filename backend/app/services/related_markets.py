from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import threading
from typing import Any

from backend.app.core.config import get_settings
from backend.app.models.contracts import MarketClickContext, RelatedMarket
from backend.app.models.scoring import RelatedMarketScoreBreakdown
from backend.app.services.cointegration import score_cointegration_pair
from backend.app.services.lagging_detector import annotate_market_status
from backend.app.storage.cache_repo import CacheRepository
from data_pipeline.market_state import mapping_market_is_concluded

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
PIPELINE_RELATED_MARKET_THRESHOLD = 0.34
METADATA_RELATED_MARKET_THRESHOLD = 0.45
MIN_RELATED_MARKETS_WITH_LOW_QUALITY_FALLBACK = 3
LOW_MATCH_QUALITY_RELATION_TYPE = "low_match_quality"
LOW_MATCH_QUALITY_NOTE = (
    "Low match quality: surfaced as a fallback because stronger related markets did not clear the relevance threshold."
)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in {"", None}:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in {"", None}:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _canonicalize_token(token: str) -> str:
    return TOKEN_ALIASES.get(token, token)


def _normalize_category(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"\s+", " ", value.strip().lower())
    return normalized or None


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


def _market_family(market_id: str, clusters: set[str], tokens: set[str] | None = None) -> str:
    market_prefix = market_id.split("-", 1)[0].upper()
    token_set = tokens or set()
    if {"temp", "temperature"} & token_set:
        if market_prefix.startswith("KXHIGH"):
            return "weather_high_temp"
        if market_prefix.startswith("KXLOW"):
            return "weather_low_temp"
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
    category: str | None = None
    event_ticker: str | None = None
    series_ticker: str | None = None


@dataclass(slots=True)
class IndexedUniverse:
    markets: dict[str, dict[str, Any]]
    profiles: dict[str, MarketProfile]
    category_index: dict[str, set[str]]
    topic_index: dict[str, set[str]]
    cluster_index: dict[str, set[str]]
    family_index: dict[str, set[str]]
    event_index: dict[str, set[str]]
    series_index: dict[str, set[str]]


@dataclass(slots=True)
class CandidateSeed:
    market_id: str
    generation_score: float
    same_category: bool = False


@dataclass(slots=True)
class PipelineData:
    universe: list[dict[str, Any]]
    metadata_by_id: dict[str, dict[str, Any]]
    pair_rows_by_market: dict[str, list[dict[str, Any]]]
    cointegration_by_pair: dict[tuple[str, str], dict[str, Any]]
    signature: tuple[str, ...]


@dataclass(slots=True)
class EvaluatedRelatedMarket:
    market: RelatedMarket
    passed_threshold: bool


def _build_market_profile(
    *,
    market_id: str,
    title: str,
    question: str,
    proxy_type: str | None = None,
    category: str | None = None,
    families: list[str] | None = None,
    tags: list[str] | None = None,
    event_ticker: str | None = None,
    series_ticker: str | None = None,
) -> MarketProfile:
    extra_text = " ".join(
        [
            category or "",
            " ".join(str(value) for value in (families or []) if value),
            " ".join(str(value) for value in (tags or []) if value),
            event_ticker or "",
            series_ticker or "",
        ]
    )
    tokens = _tokenize(f"{title} {question} {extra_text}")
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
        family=_market_family(market_id, clusters, tokens),
        category=_normalize_category(category),
        event_ticker=event_ticker.strip() if event_ticker else None,
        series_ticker=series_ticker.strip() if series_ticker else None,
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
        self.repo_root = Path(__file__).resolve().parents[3]
        self.fixture_path = Path(__file__).resolve().parents[3] / "data_pipeline" / "fixtures" / "macro_market_universe.json"
        self._indexed_universe: IndexedUniverse | None = None
        self._indexed_universe_signature: tuple[str, ...] | None = None
        self._pipeline_data: PipelineData | None = None
        self._prewarm_lock = threading.Lock()
        self._prewarm_started = False

    def _artifact_signature(self, *paths: Path) -> tuple[str, ...]:
        signature: list[str] = []
        for path in paths:
            if path.exists():
                stat = path.stat()
                signature.append(f"{path}:{stat.st_mtime_ns}:{stat.st_size}")
            else:
                signature.append(f"{path}:missing")
        return tuple(signature)

    def _pipeline_artifact_paths(self) -> dict[str, Path] | None:
        settings = get_settings()
        config_path = Path(settings.pipeline_startup_config)
        if not config_path.is_absolute():
            config_path = self.repo_root / config_path
        if not config_path.exists():
            return None
        try:
            from data_pipeline.scope import load_scope_config

            scope_config = load_scope_config(config_path)
        except Exception:
            return None
        artifact_dir = self.repo_root / "data_pipeline" / "artifacts" / scope_config.scope_slug
        published_dir = artifact_dir / "published"
        if published_dir.exists() and (
            (published_dir / "related_markets_universe.json").exists()
            or (published_dir / "market_metadata.json").exists()
        ):
            universe_path = published_dir / "related_markets_universe.json"
            metadata_path = published_dir / "market_metadata.json"
            pair_features_path = published_dir / "pair_features.csv"
            cointegration_path = published_dir / "cointegration_metrics.csv"
        else:
            universe_path = artifact_dir / "related_markets_universe.json"
            metadata_path = artifact_dir / "market_metadata.json"
            pair_features_path = artifact_dir / "pair_features.csv"
            cointegration_path = artifact_dir / "cointegration_metrics.csv"
        if not universe_path.exists() and not metadata_path.exists():
            return None
        return {
            "metadata": metadata_path,
            "universe": universe_path,
            "pair_features": pair_features_path,
            "cointegration": cointegration_path,
        }

    def _read_csv_rows(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def _proxy_type_from_metadata(self, metadata: dict[str, Any]) -> str | None:
        terms = _tokenize(
            " ".join(
                [
                    str(metadata.get("title", "")),
                    str(metadata.get("question", "")),
                    " ".join(str(value) for value in metadata.get("families", []) if value),
                    " ".join(str(value) for value in metadata.get("tags", []) if value),
                    str(metadata.get("category", "")),
                ]
            )
        )
        if "btc" in terms:
            return "btc_proxy"
        if "equities" in terms:
            return "equity_proxy"
        if "gold" in terms:
            return "cross_asset_proxy"
        if {"inflation", "rates", "labor"} & terms:
            return "rates_proxy"
        return None

    def _normalize_market_lookup_key(self, value: str | None) -> str:
        if value is None:
            return ""
        return re.sub(r"[^A-Z0-9-]", "", value.strip().upper())

    def _context_lookup_keys(self, value: str | None) -> tuple[str, ...]:
        if value is None:
            return ()

        raw = value.strip()
        if not raw:
            return ()

        ordered_candidates: list[str] = []

        def add_candidate(candidate: str) -> None:
            normalized = self._normalize_market_lookup_key(candidate)
            if normalized and normalized not in ordered_candidates:
                ordered_candidates.append(normalized)

        add_candidate(raw)

        for splitter in (":", "/"):
            parts = [part.strip() for part in raw.split(splitter) if part.strip()]
            for part in parts:
                add_candidate(part)

        return tuple(ordered_candidates)

    def _metadata_market_id(self, metadata: dict[str, Any]) -> str | None:
        market_id = str(metadata.get("market_id") or metadata.get("marketId") or "").strip()
        return market_id or None

    def _metadata_event_ticker(self, metadata: dict[str, Any]) -> str | None:
        extra = metadata.get("extra") or {}
        event_ticker = str(extra.get("event_ticker") or metadata.get("eventTicker") or "").strip()
        return event_ticker or None

    def _metadata_series_ticker(self, metadata: dict[str, Any]) -> str | None:
        extra = metadata.get("extra") or {}
        series_ticker = str(extra.get("series_ticker") or metadata.get("seriesTicker") or "").strip()
        return series_ticker or None

    def _resolve_primary_metadata(
        self,
        context: MarketClickContext,
        pipeline_data: PipelineData,
    ) -> dict[str, Any] | None:
        exact = pipeline_data.metadata_by_id.get(context.marketId)
        if exact is not None:
            return exact

        normalized_context_ids = self._context_lookup_keys(context.marketId)
        if not normalized_context_ids:
            return None
        preferred_context_id = normalized_context_ids[-1]
        normalized_context_id_set = set(normalized_context_ids)

        context_tokens = _tokenize(f"{context.marketTitle} {context.marketQuestion}")
        candidates: list[dict[str, Any]] = []
        for metadata in pipeline_data.metadata_by_id.values():
            normalized_market_id = self._normalize_market_lookup_key(self._metadata_market_id(metadata))
            normalized_event_ticker = self._normalize_market_lookup_key(self._metadata_event_ticker(metadata))
            if (
                normalized_market_id in normalized_context_id_set
                or normalized_event_ticker in normalized_context_id_set
            ):
                candidates.append(metadata)

        if not candidates:
            return None

        def candidate_rank(metadata: dict[str, Any]) -> tuple[int, int, int, int, float, str]:
            normalized_market_id = self._normalize_market_lookup_key(self._metadata_market_id(metadata))
            normalized_event_ticker = self._normalize_market_lookup_key(self._metadata_event_ticker(metadata))
            metadata_tokens = _tokenize(f"{metadata.get('title', '')} {metadata.get('question', '')}")
            overlap_score = _token_overlap_score(context_tokens, metadata_tokens)
            return (
                int(normalized_market_id == preferred_context_id),
                int(normalized_event_ticker == preferred_context_id),
                int(normalized_market_id in normalized_context_id_set),
                int(normalized_event_ticker in normalized_context_id_set),
                overlap_score,
                self._metadata_market_id(metadata) or "",
            )

        return max(candidates, key=candidate_rank)

    def _resolve_primary_market(
        self,
        context: MarketClickContext,
        pipeline_data: PipelineData | None,
    ) -> tuple[str, dict[str, Any] | None]:
        if pipeline_data is None:
            return context.marketId, None

        metadata = self._resolve_primary_metadata(context, pipeline_data)
        if metadata is None:
            return context.marketId, None

        return self._metadata_market_id(metadata) or context.marketId, metadata

    def _mirror_pair_row(self, row: dict[str, Any]) -> dict[str, Any]:
        mirrored = dict(row)
        mirrored["market_id"] = row.get("related_market_id", "")
        mirrored["related_market_id"] = row.get("market_id", "")
        mirrored["market_primary_family"] = row.get("candidate_primary_family", "")
        mirrored["candidate_primary_family"] = row.get("market_primary_family", "")
        lead_lag_direction = str(row.get("lead_lag_direction", "")).strip().lower()
        if lead_lag_direction == "primary_leads":
            mirrored["lead_lag_direction"] = "related_leads"
        elif lead_lag_direction == "related_leads":
            mirrored["lead_lag_direction"] = "primary_leads"
        residual = _safe_float(row.get("latest_residual_zscore"))
        if residual:
            mirrored["latest_residual_zscore"] = str(-residual)
        return mirrored

    def _pipeline_universe_note(
        self,
        *,
        residual_zscore: float,
        historical_comovement: float,
        cointegration_signal: bool,
        enough_history: bool,
    ) -> str:
        if not enough_history:
            return "Worth checking: metadata-derived related market candidate from the latest live refresh."
        if residual_zscore >= 2.2:
            return "Worth checking: this market looks unusually displaced versus its recent related-market baseline."
        if cointegration_signal:
            return "Worth checking: this market has a stable relationship signal in the latest pipeline refresh."
        if historical_comovement >= 0.58:
            return "Related market candidate from the latest pipeline refresh with meaningful historical comovement."
        return "Pipeline-derived related market candidate from the latest live refresh."

    def _build_pipeline_data(self, paths: dict[str, Path], signature: tuple[str, ...]) -> PipelineData | None:
        universe_records: list[dict[str, Any]] = []
        metadata_by_id: dict[str, dict[str, Any]] = {}

        if paths.get("universe") and paths["universe"].exists():
            universe_payload = json.loads(paths["universe"].read_text(encoding="utf-8"))
            universe_records = [
                record for record in universe_payload.get("records", []) if isinstance(record, dict)
            ]
        if paths["metadata"].exists():
            metadata_payload = json.loads(paths["metadata"].read_text(encoding="utf-8"))
            metadata_records = list(metadata_payload.get("records", []))
            if not metadata_records and not universe_records:
                return None
            metadata_by_id = {
                str(record.get("market_id", "")).strip(): record
                for record in metadata_records
                if str(record.get("market_id", "")).strip()
            }
        if not metadata_by_id and universe_records:
            metadata_by_id = {
                str(record.get("marketId", "")).strip(): record
                for record in universe_records
                if str(record.get("marketId", "")).strip()
            }

        if not metadata_by_id:
            return None

        concluded_market_ids = {
            market_id
            for market_id, metadata in metadata_by_id.items()
            if market_id and self._market_payload_is_concluded(metadata)
        }
        if universe_records:
            for record in universe_records:
                market_id = str(record.get("marketId", "")).strip()
                if market_id and self._market_payload_is_concluded(record, metadata_by_id.get(market_id)):
                    concluded_market_ids.add(market_id)

        pair_rows = self._read_csv_rows(paths["pair_features"])
        pair_rows_by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
        market_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "categoryScore": 0.0,
                "semanticBoost": 0.0,
                "historicalComovement": 0.0,
                "expectedReactionScore": 0.0,
                "residualZscore": 0.0,
                "enoughHistory": False,
                "cointegrationSignal": False,
            }
        )

        for raw_row in pair_rows:
            normalized_rows = [raw_row, self._mirror_pair_row(raw_row)]
            for row in normalized_rows:
                market_id = str(row.get("market_id", "")).strip()
                related_market_id = str(row.get("related_market_id", "")).strip()
                if not market_id or not related_market_id:
                    continue
                if market_id in concluded_market_ids or related_market_id in concluded_market_ids:
                    continue
                pair_rows_by_market[market_id].append(row)
                stats = market_stats[market_id]
                candidate_score = _safe_float(row.get("candidate_score"))
                semantic_score = _safe_float(row.get("semantic_similarity_score"))
                historical_score = max(
                    _safe_float(row.get("comovement_score")),
                    abs(_safe_float(row.get("return_correlation"))),
                    abs(_safe_float(row.get("quick_return_correlation"))),
                )
                expected_reaction = max(
                    historical_score,
                    _safe_float(row.get("shock_same_direction_ratio")),
                    abs(_safe_float(row.get("lead_lag_best_corr"))),
                )
                residual_zscore = abs(_safe_float(row.get("latest_residual_zscore")))
                stats["categoryScore"] = max(stats["categoryScore"], candidate_score)
                stats["semanticBoost"] = max(stats["semanticBoost"], semantic_score)
                stats["historicalComovement"] = max(stats["historicalComovement"], historical_score)
                stats["expectedReactionScore"] = max(stats["expectedReactionScore"], expected_reaction)
                stats["residualZscore"] = max(stats["residualZscore"], residual_zscore)
                stats["enoughHistory"] = bool(stats["enoughHistory"] or _safe_int(row.get("overlap_points")) >= 24)

        cointegration_rows = self._read_csv_rows(paths["cointegration"])
        cointegration_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
        for row in cointegration_rows:
            market_id = str(row.get("market_id", "")).strip()
            related_market_id = str(row.get("related_market_id", "")).strip()
            if not market_id or not related_market_id:
                continue
            if market_id in concluded_market_ids or related_market_id in concluded_market_ids:
                continue
            key = tuple(sorted((market_id, related_market_id)))
            cointegration_by_pair[key] = row
            signal = _safe_bool(row.get("spread_stationary_flag"))
            enough_history = _safe_bool(row.get("enough_history")) or _safe_bool(row.get("eligible_for_test"))
            for candidate_market_id in (market_id, related_market_id):
                stats = market_stats[candidate_market_id]
                stats["enoughHistory"] = bool(stats["enoughHistory"] or enough_history)
                stats["cointegrationSignal"] = bool(stats["cointegrationSignal"] or signal)
                if signal:
                    stats["historicalComovement"] = max(stats["historicalComovement"], 0.72)
                    stats["expectedReactionScore"] = max(stats["expectedReactionScore"], 0.74)

        if universe_records:
            universe = []
            for base_record in universe_records:
                market_id = str(base_record.get("marketId", "")).strip()
                if not market_id:
                    continue
                if market_id in concluded_market_ids:
                    continue
                stats = market_stats[market_id]
                candidate_score = max(_safe_float(base_record.get("categoryScore"), 0.25), stats["categoryScore"])
                semantic_boost = max(_safe_float(base_record.get("semanticBoost"), 0.0), stats["semanticBoost"])
                historical_comovement = max(
                    _safe_float(base_record.get("historicalComovement"), 0.15),
                    stats["historicalComovement"],
                )
                expected_reaction = max(
                    _safe_float(base_record.get("expectedReactionScore"), 0.18),
                    stats["expectedReactionScore"],
                    0.5 * candidate_score + 0.5 * historical_comovement,
                )
                residual_zscore = max(
                    abs(_safe_float(base_record.get("residualZscore"), 0.0)),
                    stats["residualZscore"],
                )
                cointegration_signal = bool(
                    _safe_bool(base_record.get("cointegrationSignal")) or stats["cointegrationSignal"]
                )
                enough_history = bool(_safe_bool(base_record.get("enoughHistory")) or stats["enoughHistory"])
                note = str(base_record.get("note") or "").strip() or self._pipeline_universe_note(
                    residual_zscore=residual_zscore,
                    historical_comovement=historical_comovement,
                    cointegration_signal=cointegration_signal,
                    enough_history=enough_history,
                )
                universe.append(
                    {
                        "marketId": market_id,
                        "title": str(base_record.get("title", market_id)),
                        "question": str(base_record.get("question", "")),
                        "category": base_record.get("category"),
                        "families": list(base_record.get("families", []) or []),
                        "tags": list(base_record.get("tags", []) or []),
                        "eventTicker": base_record.get("eventTicker"),
                        "seriesTicker": base_record.get("seriesTicker"),
                        "categoryScore": round(_clamp(candidate_score), 4),
                        "semanticBoost": round(_clamp(semantic_boost), 4),
                        "historicalComovement": round(_clamp(historical_comovement), 4),
                        "expectedReactionScore": round(_clamp(expected_reaction), 4),
                        "residualZscore": round(residual_zscore, 4),
                        "proxyType": base_record.get("proxyType") or self._proxy_type_from_metadata(base_record),
                        "note": note,
                        "enoughHistory": enough_history,
                    }
                )
        else:
            universe = []
            for market_id, metadata in metadata_by_id.items():
                if market_id in concluded_market_ids:
                    continue
                stats = market_stats[market_id]
                scope_score = _safe_float((metadata.get("extra") or {}).get("scope_score"))
                category_score = max(0.25, stats["categoryScore"], scope_score)
                semantic_boost = max(stats["semanticBoost"], min(1.0, scope_score + 0.08))
                historical_comovement = max(0.15, stats["historicalComovement"])
                expected_reaction = max(
                    0.18,
                    stats["expectedReactionScore"],
                    0.5 * category_score + 0.5 * historical_comovement,
                )
                residual_zscore = stats["residualZscore"]
                cointegration_signal = bool(stats["cointegrationSignal"])
                universe.append(
                    {
                        "marketId": market_id,
                        "title": str(metadata.get("title", market_id)),
                        "question": str(metadata.get("question", "")),
                        "category": metadata.get("category"),
                        "families": list(metadata.get("families", []) or []),
                        "tags": list(metadata.get("tags", []) or []),
                        "eventTicker": (metadata.get("extra") or {}).get("event_ticker"),
                        "seriesTicker": (metadata.get("extra") or {}).get("series_ticker"),
                        "categoryScore": round(_clamp(category_score), 4),
                        "semanticBoost": round(_clamp(semantic_boost), 4),
                        "historicalComovement": round(_clamp(historical_comovement), 4),
                        "expectedReactionScore": round(_clamp(expected_reaction), 4),
                        "residualZscore": round(residual_zscore, 4),
                        "proxyType": self._proxy_type_from_metadata(metadata),
                        "note": self._pipeline_universe_note(
                            residual_zscore=residual_zscore,
                            historical_comovement=historical_comovement,
                            cointegration_signal=cointegration_signal,
                            enough_history=bool(stats["enoughHistory"]),
                        ),
                        "enoughHistory": bool(stats["enoughHistory"]),
                    }
                )

        return PipelineData(
            universe=universe,
            metadata_by_id=metadata_by_id,
            pair_rows_by_market=dict(pair_rows_by_market),
            cointegration_by_pair=cointegration_by_pair,
            signature=signature,
        )

    def _load_pipeline_data(self) -> PipelineData | None:
        if self.universe_override is not None:
            return None
        paths = self._pipeline_artifact_paths()
        if not paths:
            return None
        signature = self._artifact_signature(
            paths["universe"],
            paths["metadata"],
            paths["pair_features"],
            paths["cointegration"],
        )
        if self._pipeline_data is not None and self._pipeline_data.signature == signature:
            return self._pipeline_data
        pipeline_data = self._build_pipeline_data(paths, signature)
        self._pipeline_data = pipeline_data
        return pipeline_data

    def prewarm(self) -> None:
        if self.universe_override is not None:
            return
        with self._prewarm_lock:
            if self._prewarm_started:
                return
            self._prewarm_started = True
        try:
            self._load_pipeline_data()
            self._indexed_fixture()
        except Exception:
            pass

    def _load_fixture(self) -> list[dict[str, Any]]:
        if self.universe_override is not None:
            return self.universe_override

        pipeline_data = self._load_pipeline_data()
        if pipeline_data is not None and pipeline_data.universe:
            return pipeline_data.universe

        cached = self.cache_repo.get_json("fixture", "macro_market_universe", max_age_seconds=60)
        if cached is None:
            with self.fixture_path.open("r", encoding="utf-8") as fixture_file:
                cached = json.load(fixture_file)
            self.cache_repo.set_json("fixture", "macro_market_universe", cached)
        return cached

    def _market_payload_is_concluded(
        self,
        payload: dict[str, Any] | None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        return mapping_market_is_concluded(metadata) or mapping_market_is_concluded(payload)

    def _indexed_fixture(self) -> IndexedUniverse:
        if self.universe_override is not None:
            source_signature = ("override", str(id(self.universe_override)))
        else:
            pipeline_data = self._load_pipeline_data()
            if pipeline_data is not None and pipeline_data.universe:
                source_signature = pipeline_data.signature
            else:
                source_signature = self._artifact_signature(self.fixture_path)

        if self._indexed_universe is not None and self._indexed_universe_signature == source_signature:
            return self._indexed_universe

        markets: dict[str, dict[str, Any]] = {}
        profiles: dict[str, MarketProfile] = {}
        category_index: dict[str, set[str]] = defaultdict(set)
        topic_index: dict[str, set[str]] = defaultdict(set)
        cluster_index: dict[str, set[str]] = defaultdict(set)
        family_index: dict[str, set[str]] = defaultdict(set)
        event_index: dict[str, set[str]] = defaultdict(set)
        series_index: dict[str, set[str]] = defaultdict(set)

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
                category=str(candidate.get("category", "")) if candidate.get("category") else None,
                families=[str(value) for value in candidate.get("families", []) if value],
                tags=[str(value) for value in candidate.get("tags", []) if value],
                event_ticker=str(candidate.get("eventTicker", "")) if candidate.get("eventTicker") else None,
                series_ticker=str(candidate.get("seriesTicker", "")) if candidate.get("seriesTicker") else None,
            )
            markets[market_id] = candidate
            profiles[market_id] = profile
            if profile.category:
                category_index[profile.category].add(market_id)
            family_index[profile.family].add(market_id)
            if profile.event_ticker:
                event_index[profile.event_ticker].add(market_id)
            if profile.series_ticker:
                series_index[profile.series_ticker].add(market_id)
            for topic in profile.topics:
                topic_index[topic].add(market_id)
            for cluster in profile.clusters:
                cluster_index[cluster].add(market_id)

        self._indexed_universe = IndexedUniverse(
            markets=markets,
            profiles=profiles,
            category_index=dict(category_index),
            topic_index=dict(topic_index),
            cluster_index=dict(cluster_index),
            family_index=dict(family_index),
            event_index=dict(event_index),
            series_index=dict(series_index),
        )
        self._indexed_universe_signature = source_signature
        return self._indexed_universe

    def _pipeline_note_for_pair(
        self,
        row: dict[str, Any],
        *,
        residual_zscore: float,
        cointegration_signal: bool,
    ) -> str:
        if residual_zscore >= 2.2:
            return "Worth checking: this relationship is unusually stretched versus the latest pipeline baseline."
        lead_lag_direction = str(row.get("lead_lag_direction", "")).strip().lower()
        if lead_lag_direction == "primary_leads":
            return "Worth checking: this market has recently lagged the primary market in related moves."
        if lead_lag_direction == "related_leads":
            return "This market has sometimes moved ahead of the primary market in recent related moves."
        if cointegration_signal:
            return "Related market with a stable relationship signal in the latest pipeline refresh."
        shared_terms = [term for term in str(row.get("shared_terms", "")).split("|") if term]
        if shared_terms:
            return f"Related via shared themes: {', '.join(shared_terms[:4])}."
        return "Pipeline-derived related market from the latest live refresh."

    def _mark_low_match_quality(self, market: RelatedMarket) -> RelatedMarket:
        return market.model_copy(
            update={
                "relationTypes": _dedupe_preserving_order(
                    [*market.relationTypes, LOW_MATCH_QUALITY_RELATION_TYPE]
                ),
                "status": "normal",
                "note": LOW_MATCH_QUALITY_NOTE,
            }
        )

    def _finalize_related_markets_with_low_quality(
        self,
        strong_markets: list[RelatedMarket],
        low_quality_markets: list[RelatedMarket],
        *,
        sibling_group_by_market: dict[str, str | None] | None = None,
        limit: int = 5,
    ) -> list[RelatedMarket]:
        finalized = self._finalize_related_markets(
            strong_markets,
            sibling_group_by_market=sibling_group_by_market,
            limit=limit,
        )
        unique_strong = self._diversify_related_markets(
            finalized,
            sibling_group_by_market=sibling_group_by_market,
            limit=limit,
            backfill_with_leftovers=False,
        )
        if len(unique_strong) >= MIN_RELATED_MARKETS_WITH_LOW_QUALITY_FALLBACK or not low_quality_markets:
            return finalized

        supplemental = self._finalize_related_markets(
            low_quality_markets,
            sibling_group_by_market=sibling_group_by_market,
            limit=MIN_RELATED_MARKETS_WITH_LOW_QUALITY_FALLBACK,
        )
        unique_supplemental = self._diversify_related_markets(
            supplemental,
            sibling_group_by_market=sibling_group_by_market,
            limit=MIN_RELATED_MARKETS_WITH_LOW_QUALITY_FALLBACK,
            backfill_with_leftovers=False,
        )

        seen_market_ids = {market.marketId for market in unique_strong}
        combined = list(unique_strong)
        for market in unique_supplemental:
            if market.marketId in seen_market_ids:
                continue
            combined.append(market)
            seen_market_ids.add(market.marketId)
            if len(combined) >= MIN_RELATED_MARKETS_WITH_LOW_QUALITY_FALLBACK:
                break

        if len(combined) >= limit:
            return combined[:limit]

        prioritized_leftovers = [*finalized, *supplemental]
        for market in prioritized_leftovers:
            if market.marketId in seen_market_ids:
                continue
            combined.append(market)
            seen_market_ids.add(market.marketId)
            if len(combined) >= limit:
                break
        return combined[:limit]

    def _build_pipeline_related_market(
        self,
        *,
        context: MarketClickContext,
        primary_profile: MarketProfile,
        candidate_metadata: dict[str, Any],
        candidate_profile: MarketProfile,
        row: dict[str, Any],
        cointegration_row: dict[str, Any] | None,
    ) -> EvaluatedRelatedMarket:
        category_score = _clamp(
            max(
                _safe_float(row.get("candidate_score")),
                _safe_float(row.get("category_overlap_score")),
                _safe_float(row.get("family_alignment_score")),
            )
        )
        topic_score = _soft_set_similarity(primary_profile.topics, candidate_profile.topics)
        semantic_similarity = _clamp(
            max(
                _safe_float(row.get("semantic_similarity_score")),
                _token_overlap_score(primary_profile.tokens, candidate_profile.tokens),
            )
        )
        historical_comovement = _clamp(
            max(
                _safe_float(row.get("comovement_score")),
                abs(_safe_float(row.get("return_correlation"))),
                abs(_safe_float(row.get("quick_return_correlation"))),
            )
        )
        enough_history = (
            _safe_bool((cointegration_row or {}).get("enough_history"))
            or _safe_bool((cointegration_row or {}).get("eligible_for_test"))
            or _safe_int(row.get("overlap_points")) >= 24
        )
        cointegration_bonus = score_cointegration_pair(
            context.marketId,
            candidate_profile.market_id,
            enough_history=enough_history,
            category_score=category_score,
            topic_score=topic_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            primary_topics=primary_profile.topics,
            related_topics=candidate_profile.topics,
        )
        if _safe_bool((cointegration_row or {}).get("spread_stationary_flag")):
            cointegration_bonus = max(cointegration_bonus, 0.08)
        score = RelatedMarketScoreBreakdown(
            category_score=category_score,
            topic_score=topic_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            cointegration_bonus=cointegration_bonus,
        )

        same_event = bool(
            primary_profile.event_ticker
            and candidate_profile.event_ticker
            and primary_profile.event_ticker == candidate_profile.event_ticker
        )
        same_series = bool(
            primary_profile.series_ticker
            and candidate_profile.series_ticker
            and primary_profile.series_ticker == candidate_profile.series_ticker
        )
        relation_types: list[str] = []
        if same_event:
            relation_types.append("same_event")
        if same_series:
            relation_types.append("same_series")
        if not _safe_bool(row.get("cross_family_link")):
            relation_types.append("macro_cluster")
        if topic_score >= 0.55:
            relation_types.append("topic_match")
        if semantic_similarity >= 0.5:
            relation_types.append("semantic_similarity")
        if historical_comovement >= 0.55:
            relation_types.append("historical_comovement")
        if cointegration_bonus > 0:
            relation_types.append("cointegration_signal")
        lead_lag_direction = str(row.get("lead_lag_direction", "")).strip().lower()
        if lead_lag_direction in {"primary_leads", "related_leads", "synchronous"}:
            relation_types.append(lead_lag_direction)
        proxy_type = self._proxy_type_from_metadata(candidate_metadata)
        if proxy_type:
            relation_types.append(proxy_type)

        residual_zscore = _safe_float(row.get("latest_residual_zscore"))
        note = self._pipeline_note_for_pair(
            row,
            residual_zscore=abs(residual_zscore),
            cointegration_signal=cointegration_bonus > 0,
        )
        market = annotate_market_status(
            RelatedMarket(
                marketId=candidate_profile.market_id,
                title=str(candidate_metadata.get("title", candidate_profile.market_id)),
                relationTypes=_dedupe_preserving_order(relation_types or ["historical_comovement"]),
                relationStrength=score.total,
                expectedReactionScore=max(
                    historical_comovement,
                    _safe_float(row.get("shock_same_direction_ratio")),
                    abs(_safe_float(row.get("lead_lag_best_corr"))),
                ),
                residualZscore=residual_zscore,
                note=note,
            ),
            category_score=category_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            cointegration_bonus=cointegration_bonus,
        )
        passed_threshold = score.total >= PIPELINE_RELATED_MARKET_THRESHOLD
        if not passed_threshold:
            market = self._mark_low_match_quality(market)
        return EvaluatedRelatedMarket(market=market, passed_threshold=passed_threshold)

    def _find_pipeline_related_markets(self, context: MarketClickContext) -> list[RelatedMarket]:
        pipeline_data = self._load_pipeline_data()
        if pipeline_data is None:
            return []
        primary_market_id, _ = self._resolve_primary_market(context, pipeline_data)
        primary_profile = self._primary_profile(context)
        rows = list(pipeline_data.pair_rows_by_market.get(primary_market_id, []))
        if not rows:
            return []

        related: list[RelatedMarket] = []
        low_quality_related: list[RelatedMarket] = []
        sibling_group_by_market: dict[str, str | None] = {}
        for row in rows:
            candidate_market_id = str(row.get("related_market_id", "")).strip()
            candidate_metadata = pipeline_data.metadata_by_id.get(candidate_market_id)
            if candidate_metadata is None:
                continue
            if self._market_payload_is_concluded(candidate_metadata):
                continue
            candidate_profile = _build_market_profile(
                market_id=candidate_market_id,
                title=str(candidate_metadata.get("title", candidate_market_id)),
                question=str(candidate_metadata.get("question", "")),
                proxy_type=self._proxy_type_from_metadata(candidate_metadata),
                category=str(candidate_metadata.get("category", "")) if candidate_metadata.get("category") else None,
                families=[str(value) for value in candidate_metadata.get("families", []) if value],
                tags=[str(value) for value in candidate_metadata.get("tags", []) if value],
                event_ticker=self._metadata_event_ticker(candidate_metadata),
                series_ticker=self._metadata_series_ticker(candidate_metadata),
            )
            cointegration_row = pipeline_data.cointegration_by_pair.get(tuple(sorted((primary_market_id, candidate_market_id))))
            evaluated_market = self._build_pipeline_related_market(
                context=context,
                primary_profile=primary_profile,
                candidate_metadata=candidate_metadata,
                candidate_profile=candidate_profile,
                row=row,
                cointegration_row=cointegration_row,
            )
            sibling_group_by_market[evaluated_market.market.marketId] = candidate_profile.event_ticker
            if evaluated_market.passed_threshold:
                related.append(evaluated_market.market)
            else:
                low_quality_related.append(evaluated_market.market)

        deduped_related: dict[str, RelatedMarket] = {}
        for market in sorted(related, key=lambda item: item.relationStrength, reverse=True):
            deduped_related.setdefault(market.marketId, market)
        deduped_low_quality: dict[str, RelatedMarket] = {}
        for market in sorted(low_quality_related, key=lambda item: item.relationStrength, reverse=True):
            deduped_low_quality.setdefault(market.marketId, market)
        return self._finalize_related_markets_with_low_quality(
            list(deduped_related.values()),
            list(deduped_low_quality.values()),
            sibling_group_by_market=sibling_group_by_market,
        )

    def _has_strong_cross_event_option(self, markets: list[RelatedMarket]) -> bool:
        return any(
            "same_event" not in market.relationTypes
            and (
                "same_series" in market.relationTypes
                or "historical_comovement" in market.relationTypes
                or "cointegration_signal" in market.relationTypes
                or market.relationStrength >= 0.65
            )
            for market in markets
        )

    def _diversify_related_markets(
        self,
        markets: list[RelatedMarket],
        *,
        sibling_group_by_market: dict[str, str | None] | None = None,
        limit: int = 5,
        backfill_with_leftovers: bool = True,
    ) -> list[RelatedMarket]:
        if not markets:
            return []

        sibling_group_by_market = sibling_group_by_market or {}

        def group_key(market: RelatedMarket) -> str:
            sibling_group = str(sibling_group_by_market.get(market.marketId) or "").strip()
            return sibling_group or f"market:{market.marketId}"

        selected: list[RelatedMarket] = []
        leftovers: list[RelatedMarket] = []
        seen_groups: set[str] = set()

        for market in markets:
            key = group_key(market)
            if key in seen_groups:
                leftovers.append(market)
                continue
            seen_groups.add(key)
            selected.append(market)
            if len(selected) >= limit:
                return selected[:limit]

        if backfill_with_leftovers and len(selected) < limit:
            selected.extend(leftovers[: limit - len(selected)])

        return selected[:limit]

    def _finalize_related_markets(
        self,
        markets: list[RelatedMarket],
        *,
        sibling_group_by_market: dict[str, str | None] | None = None,
        limit: int = 5,
    ) -> list[RelatedMarket]:
        ranked = sorted(markets, key=lambda item: item.relationStrength, reverse=True)
        if not ranked:
            return []

        cross_event = [market for market in ranked if "same_event" not in market.relationTypes]
        same_event = [market for market in ranked if "same_event" in market.relationTypes]
        if self._has_strong_cross_event_option(ranked):
            return self._diversify_related_markets(
                cross_event,
                sibling_group_by_market=sibling_group_by_market,
                limit=limit,
            )
        if same_event:
            return self._diversify_related_markets(
                same_event,
                sibling_group_by_market=sibling_group_by_market,
                limit=limit,
            )
        return self._diversify_related_markets(
            ranked,
            sibling_group_by_market=sibling_group_by_market,
            limit=limit,
        )

    def _primary_profile(self, context: MarketClickContext) -> MarketProfile:
        pipeline_data = self._load_pipeline_data()
        resolved_market_id, metadata = self._resolve_primary_market(context, pipeline_data)
        if metadata is not None:
            return _build_market_profile(
                market_id=resolved_market_id,
                title=str(metadata.get("title", context.marketTitle)),
                question=str(metadata.get("question", context.marketQuestion)),
                proxy_type=self._proxy_type_from_metadata(metadata),
                category=str(metadata.get("category", "")) if metadata.get("category") else None,
                families=[str(value) for value in metadata.get("families", []) if value],
                tags=[str(value) for value in metadata.get("tags", []) if value],
                event_ticker=self._metadata_event_ticker(metadata),
                series_ticker=self._metadata_series_ticker(metadata),
            )
        return _build_market_profile(
            market_id=context.marketId,
            title=context.marketTitle,
            question=context.marketQuestion,
        )

    def _generate_candidate_seeds(
        self,
        primary_profile: MarketProfile,
        indexed_universe: IndexedUniverse,
    ) -> tuple[list[CandidateSeed], list[CandidateSeed]]:
        candidate_weights: dict[str, float] = {}

        def add_candidate(market_id: str, weight: float) -> None:
            if market_id == primary_profile.market_id:
                return
            candidate_weights[market_id] = max(candidate_weights.get(market_id, 0.0), weight)

        if primary_profile.event_ticker:
            for market_id in indexed_universe.event_index.get(primary_profile.event_ticker, set()):
                add_candidate(market_id, 1.0)

        if primary_profile.series_ticker:
            for market_id in indexed_universe.series_index.get(primary_profile.series_ticker, set()):
                add_candidate(market_id, 0.97)

        if primary_profile.category:
            for market_id in indexed_universe.category_index.get(primary_profile.category, set()):
                add_candidate(market_id, 0.93)

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

        preferred_seeds: list[CandidateSeed] = []
        fallback_seeds: list[CandidateSeed] = []
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
            seed = CandidateSeed(
                market_id=market_id,
                generation_score=round(
                    _clamp(max(generation_score, 0.55 * topic_similarity + 0.45 * category_similarity)),
                    4,
                ),
                same_category=bool(
                    primary_profile.category
                    and candidate_profile.category
                    and primary_profile.category == candidate_profile.category
                ),
            )
            if (
                seed.same_category
                or (
                    primary_profile.event_ticker
                    and candidate_profile.event_ticker
                    and primary_profile.event_ticker == candidate_profile.event_ticker
                )
                or (
                    primary_profile.series_ticker
                    and candidate_profile.series_ticker
                    and primary_profile.series_ticker == candidate_profile.series_ticker
                )
            ):
                preferred_seeds.append(seed)
            else:
                fallback_seeds.append(seed)

        return preferred_seeds[:16], fallback_seeds[:16]

    def _score_candidate(
        self,
        primary_profile: MarketProfile,
        candidate: dict[str, Any],
        candidate_profile: MarketProfile,
        *,
        generation_score: float,
    ) -> EvaluatedRelatedMarket:
        category_prior = float(candidate.get("categoryScore", 0.35))
        semantic_prior = float(candidate.get("semanticBoost", 0.0))
        historical_prior = float(candidate.get("historicalComovement", 0.35))
        same_event = bool(
            primary_profile.event_ticker
            and candidate_profile.event_ticker
            and primary_profile.event_ticker == candidate_profile.event_ticker
        )
        same_series = bool(
            primary_profile.series_ticker
            and candidate_profile.series_ticker
            and primary_profile.series_ticker == candidate_profile.series_ticker
        )
        same_category = bool(
            primary_profile.category
            and candidate_profile.category
            and primary_profile.category == candidate_profile.category
        )

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
        if same_event:
            topic_score = max(topic_score, 1.0)
            category_score = max(category_score, 0.95)
            semantic_similarity = max(semantic_similarity, 0.82)
        elif same_series:
            topic_score = max(topic_score, 0.9)
            category_score = max(category_score, 0.82)
            semantic_similarity = max(semantic_similarity, 0.68)
        elif same_category:
            topic_score = max(topic_score, 0.72)
            category_score = max(category_score, 0.7)
            semantic_similarity = max(semantic_similarity, 0.54)
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

        relation_types: list[str] = []
        if primary_profile.event_ticker and primary_profile.event_ticker == candidate_profile.event_ticker:
            relation_types.append("same_event")
        if primary_profile.series_ticker and primary_profile.series_ticker == candidate_profile.series_ticker:
            relation_types.append("same_series")
        if same_category:
            relation_types.append("same_category")
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

        note = str(candidate.get("note")) if candidate.get("note") else None
        if not bool(candidate.get("enoughHistory", False)) and note and "worth checking" not in note.lower():
            note = f"Worth checking: {note[0].lower() + note[1:]}" if note else note

        market = annotate_market_status(
            RelatedMarket(
                marketId=candidate_profile.market_id,
                title=str(candidate.get("title", candidate_profile.market_id)),
                relationTypes=_dedupe_preserving_order(relation_types or ["semantic_similarity"]),
                relationStrength=score.total,
                expectedReactionScore=float(candidate.get("expectedReactionScore", historical_comovement)),
                residualZscore=float(candidate.get("residualZscore", 0.0)),
                note=note,
            ),
            category_score=category_score,
            semantic_similarity=semantic_similarity,
            historical_comovement=historical_comovement,
            cointegration_bonus=cointegration_bonus,
        )
        passed_threshold = score.total >= METADATA_RELATED_MARKET_THRESHOLD
        if not passed_threshold:
            market = self._mark_low_match_quality(market)
        return EvaluatedRelatedMarket(market=market, passed_threshold=passed_threshold)

    def find_related_markets(self, context: MarketClickContext) -> list[RelatedMarket]:
        pipeline_related = self._find_pipeline_related_markets(context)
        if pipeline_related:
            return pipeline_related

        indexed_universe = self._indexed_fixture()
        primary_profile = indexed_universe.profiles.get(context.marketId) or self._primary_profile(context)
        related: list[RelatedMarket] = []
        low_quality_related: list[RelatedMarket] = []
        sibling_group_by_market: dict[str, str | None] = {}

        preferred_seeds, fallback_seeds = self._generate_candidate_seeds(primary_profile, indexed_universe)

        def is_same_event_seed(seed: CandidateSeed) -> bool:
            candidate_profile = indexed_universe.profiles[seed.market_id]
            return bool(
                primary_profile.event_ticker
                and candidate_profile.event_ticker
                and primary_profile.event_ticker == candidate_profile.event_ticker
            )

        cross_event_preferred = [seed for seed in preferred_seeds if not is_same_event_seed(seed)]
        same_event_preferred = [seed for seed in preferred_seeds if is_same_event_seed(seed)]
        cross_event_fallback = [seed for seed in fallback_seeds if not is_same_event_seed(seed)]
        same_event_fallback = [seed for seed in fallback_seeds if is_same_event_seed(seed)]

        def score_seed_batch(seeds: list[CandidateSeed]) -> None:
            for seed in seeds:
                candidate = indexed_universe.markets[seed.market_id]
                evaluated_market = self._score_candidate(
                    primary_profile,
                    candidate,
                    indexed_universe.profiles[seed.market_id],
                    generation_score=seed.generation_score,
                )
                sibling_group_by_market[evaluated_market.market.marketId] = indexed_universe.profiles[
                    seed.market_id
                ].event_ticker
                if evaluated_market.passed_threshold:
                    related.append(evaluated_market.market)
                else:
                    low_quality_related.append(evaluated_market.market)

        score_seed_batch(cross_event_preferred)
        score_seed_batch(cross_event_fallback)
        if not self._has_strong_cross_event_option(related):
            score_seed_batch(same_event_preferred)
            score_seed_batch(same_event_fallback)

        return self._finalize_related_markets_with_low_quality(
            related,
            low_quality_related,
            sibling_group_by_market=sibling_group_by_market,
        )
