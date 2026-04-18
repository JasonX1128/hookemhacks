from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable

from .common import CONFIG_ROOT, SCOPE_SCHEMA_VERSION
from .schemas import MarketMetadataRecord
from .utils import build_json_envelope, clamp, normalize_text, parse_timestamp, semantic_similarity, tokenize_text, write_json


DEFAULT_TARGET_FAMILIES = [
    "inflation",
    "federal_reserve",
    "monetary_policy",
    "labor_market",
    "jobs",
    "economic_growth",
    "interest_rates",
]
DEFAULT_TOPIC_SEEDS = [
    "CPI",
    "inflation",
    "core inflation",
    "FOMC",
    "Fed",
    "rate cut",
    "rate hike",
    "interest rates",
    "unemployment",
    "jobs report",
    "nonfarm payrolls",
    "GDP",
    "recession",
    "PCE",
    "wage growth",
    "labor force participation",
    "treasury yields",
    "economic outlook",
]


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item) for item in value]


@dataclass(slots=True)
class PipelineScopeConfig:
    scope_id: str
    description: str | None = None
    provider_name: str | None = None
    target_families: list[str] = field(default_factory=list)
    topic_seeds: list[str] = field(default_factory=list)
    window_start: str | None = None
    window_end: str | None = None
    max_markets: int | None = 20
    per_family_limit: int | None = 6
    top_k: int = 5
    max_pool_size: int = 40
    cross_family_semantic_min: float = 0.58
    min_seed_semantic_similarity: float = 0.3

    def __post_init__(self) -> None:
        self.target_families = _normalize_string_list(self.target_families)
        self.topic_seeds = _normalize_string_list(self.topic_seeds, preserve_case=True)
        if not self.target_families and not self.topic_seeds:
            raise ValueError("scope config must include at least one target family or topic seed")

    @property
    def scope_slug(self) -> str:
        normalized = normalize_text(self.scope_id).replace(" ", "_")
        normalized = "_".join(part for part in normalized.split("_") if part)
        return normalized or "default"

    @property
    def normalized_target_families(self) -> list[str]:
        return _normalize_string_list(self.target_families)

    @property
    def normalized_topic_seeds(self) -> list[str]:
        return [normalize_text(seed) for seed in self.topic_seeds if normalize_text(seed)]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "PipelineScopeConfig":
        return cls(
            scope_id=str(payload.get("scope_id") or payload.get("scopeId") or "default"),
            description=payload.get("description"),
            provider_name=payload.get("provider_name") or payload.get("provider"),
            target_families=_coerce_list(payload.get("target_families") or payload.get("targetFamilies")),
            topic_seeds=_coerce_list(payload.get("topic_seeds") or payload.get("topicSeeds")),
            window_start=payload.get("window_start") or payload.get("windowStart"),
            window_end=payload.get("window_end") or payload.get("windowEnd"),
            max_markets=payload.get("max_markets") if payload.get("max_markets") is not None else payload.get("maxMarkets"),
            per_family_limit=payload.get("per_family_limit") if payload.get("per_family_limit") is not None else payload.get("perFamilyLimit"),
            top_k=payload.get("top_k") if payload.get("top_k") is not None else payload.get("topK", 5),
            max_pool_size=payload.get("max_pool_size") if payload.get("max_pool_size") is not None else payload.get("maxPoolSize", 40),
            cross_family_semantic_min=payload.get("cross_family_semantic_min")
            if payload.get("cross_family_semantic_min") is not None
            else payload.get("crossFamilySemanticMin", 0.58),
            min_seed_semantic_similarity=payload.get("min_seed_semantic_similarity")
            if payload.get("min_seed_semantic_similarity") is not None
            else payload.get("minSeedSemanticSimilarity", 0.3),
        )


@dataclass(slots=True)
class ScopeSelection:
    include: bool
    score: float
    matched_families: list[str]
    matched_topic_seeds: list[str]
    seed_semantic_similarity: float
    time_window_overlap_score: float
    primary_family: str | None
    family_terms: list[str]


def default_scope_config(provider_name: str | None = None) -> PipelineScopeConfig:
    return PipelineScopeConfig(
        scope_id="macro_default",
        description="Default scoped macro market universe for inflation, Fed, jobs, growth, and rates analysis.",
        provider_name=provider_name or "mock",
        target_families=list(DEFAULT_TARGET_FAMILIES),
        topic_seeds=list(DEFAULT_TOPIC_SEEDS),
        window_start="2026-01-01T00:00:00Z",
        window_end="2027-03-31T23:59:59Z",
        max_markets=24,
        per_family_limit=5,
        top_k=5,
        max_pool_size=30,
        cross_family_semantic_min=0.52,
        min_seed_semantic_similarity=0.28,
    )


def default_scope_config_path() -> Path:
    return CONFIG_ROOT / "macro_default.json"


def load_scope_config(config_path: Path | None = None, *, provider_name: str | None = None) -> PipelineScopeConfig:
    if config_path is None:
        return default_scope_config(provider_name=provider_name)
    if config_path.suffix.lower() != ".json":
        raise ValueError(f"unsupported scope config format for {config_path}; JSON is supported")
    import json

    with config_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    scope_config = PipelineScopeConfig.from_mapping(payload)
    if provider_name and not scope_config.provider_name:
        scope_config = replace(scope_config, provider_name=provider_name)
    return scope_config


def apply_cli_overrides(
    scope_config: PipelineScopeConfig,
    *,
    provider_name: str | None = None,
    scope_id: str | None = None,
    families: list[str] | None = None,
    topic_seeds: list[str] | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    max_markets: int | None = None,
    per_family_limit: int | None = None,
    top_k: int | None = None,
    max_pool_size: int | None = None,
    cross_family_semantic_min: float | None = None,
    min_seed_semantic_similarity: float | None = None,
) -> PipelineScopeConfig:
    updated = replace(scope_config)
    if provider_name:
        updated.provider_name = provider_name
    if scope_id:
        updated.scope_id = scope_id
    if families:
        updated.target_families = _normalize_string_list(families)
    if topic_seeds:
        updated.topic_seeds = _normalize_string_list(topic_seeds, preserve_case=True)
    if window_start is not None:
        updated.window_start = window_start
    if window_end is not None:
        updated.window_end = window_end
    if max_markets is not None:
        updated.max_markets = max_markets
    if per_family_limit is not None:
        updated.per_family_limit = per_family_limit
    if top_k is not None:
        updated.top_k = top_k
    if max_pool_size is not None:
        updated.max_pool_size = max_pool_size
    if cross_family_semantic_min is not None:
        updated.cross_family_semantic_min = cross_family_semantic_min
    if min_seed_semantic_similarity is not None:
        updated.min_seed_semantic_similarity = min_seed_semantic_similarity
    updated.__post_init__()
    return updated


def add_scope_arguments(parser: Any) -> None:
    parser.add_argument("--config", type=Path, default=None, help="Optional JSON config file describing the scoped market universe.")
    parser.add_argument("--provider", default=None, help="Data provider name. Supported: mock, snapshot, kalshi_live.")
    parser.add_argument("--scope-id", default=None, help="Optional scope id used in artifact/cache directory names.")
    parser.add_argument(
        "--family",
        action="append",
        default=None,
        help="Repeatable target family override. Comma-separated values are also accepted.",
    )
    parser.add_argument(
        "--seed",
        action="append",
        default=None,
        help="Repeatable topic seed override. Comma-separated values are also accepted.",
    )
    parser.add_argument("--window-start", default=None, help="Optional UTC start bound for scoped market relevance.")
    parser.add_argument("--window-end", default=None, help="Optional UTC end bound for scoped market relevance.")
    parser.add_argument("--max-markets", type=int, default=None, help="Optional total cap on selected markets in scope.")
    parser.add_argument("--per-family-limit", type=int, default=None, help="Optional cap per primary family within scope.")
    parser.add_argument("--top-k", type=int, default=None, help="Optional override for top candidate count per market.")
    parser.add_argument("--max-pool-size", type=int, default=None, help="Optional override for metadata-derived candidate pool size.")
    parser.add_argument(
        "--cross-family-semantic-min",
        type=float,
        default=None,
        help="Minimum semantic similarity required for cross-family candidate links.",
    )
    parser.add_argument(
        "--min-seed-semantic-similarity",
        type=float,
        default=None,
        help="Minimum semantic match to keep a seed-only market in the scoped universe.",
    )


def resolve_scope_from_args(args: Any) -> tuple[str, PipelineScopeConfig]:
    provider_name = getattr(args, "provider", None)
    config_path = getattr(args, "config", None)
    base_scope = load_scope_config(config_path, provider_name=provider_name)
    provider_name = provider_name or base_scope.provider_name or "mock"
    scope_config = apply_cli_overrides(
        base_scope,
        provider_name=provider_name,
        scope_id=getattr(args, "scope_id", None),
        families=_explode_cli_list(getattr(args, "family", None)),
        topic_seeds=_explode_cli_list(getattr(args, "seed", None)),
        window_start=getattr(args, "window_start", None),
        window_end=getattr(args, "window_end", None),
        max_markets=getattr(args, "max_markets", None),
        per_family_limit=getattr(args, "per_family_limit", None),
        top_k=getattr(args, "top_k", None),
        max_pool_size=getattr(args, "max_pool_size", None),
        cross_family_semantic_min=getattr(args, "cross_family_semantic_min", None),
        min_seed_semantic_similarity=getattr(args, "min_seed_semantic_similarity", None),
    )
    return provider_name, scope_config


def persist_scope_artifact(*, path: Path, provider_name: str, scope_config: PipelineScopeConfig) -> None:
    payload = build_json_envelope(
        artifact_name="run_scope",
        provider_name=provider_name,
        schema_version=SCOPE_SCHEMA_VERSION,
        record_key="records",
        records=[],
        extra={"scope": scope_config.to_dict()},
    )
    write_json(path, payload)


def select_scoped_markets(
    records: list[MarketMetadataRecord],
    scope_config: PipelineScopeConfig,
) -> tuple[list[MarketMetadataRecord], dict[str, Any]]:
    selections: list[tuple[MarketMetadataRecord, ScopeSelection]] = []
    for record in records:
        selection = evaluate_scope_match(record, scope_config)
        if selection.include:
            annotated = MarketMetadataRecord.from_mapping(record.to_dict())
            annotated.extra = {
                **annotated.extra,
                "scope_id": scope_config.scope_id,
                "scope_score": round(selection.score, 4),
                "scope_primary_family": selection.primary_family,
                "matched_scope_families": selection.matched_families,
                "matched_topic_seeds": selection.matched_topic_seeds,
                "seed_semantic_similarity": round(selection.seed_semantic_similarity, 4),
                "time_window_overlap_score": round(selection.time_window_overlap_score, 4),
                "family_terms": selection.family_terms,
            }
            selections.append((annotated, selection))

    selections.sort(key=lambda item: (-item[1].score, item[0].market_id))
    family_counts: Counter[str] = Counter()
    selected_records: list[MarketMetadataRecord] = []
    for record, selection in selections:
        bucket = selection.primary_family or "seed_only"
        if scope_config.per_family_limit is not None and family_counts[bucket] >= scope_config.per_family_limit:
            continue
        selected_records.append(record)
        family_counts[bucket] += 1
        if scope_config.max_markets is not None and len(selected_records) >= scope_config.max_markets:
            break

    summary = {
        "scope": scope_config.to_dict(),
        "selected_market_count": len(selected_records),
        "family_counts": dict(sorted(family_counts.items())),
        "selected_market_ids": [record.market_id for record in selected_records],
    }
    return selected_records, summary


def evaluate_scope_match(record: MarketMetadataRecord, scope_config: PipelineScopeConfig) -> ScopeSelection:
    family_terms = market_family_terms(record)
    matched_families = [family for family in scope_config.normalized_target_families if family in family_terms]
    record_tokens = tokenize_text(record.title, record.question, record.category, " ".join(record.families), " ".join(record.tags))
    matched_topic_seeds: list[str] = []
    seed_semantic_similarity = 0.0
    for seed in scope_config.topic_seeds:
        seed_tokens = tokenize_text(seed)
        if seed_tokens and seed_tokens <= record_tokens:
            matched_topic_seeds.append(seed)
        seed_semantic_similarity = max(seed_semantic_similarity, semantic_similarity(record.combined_text, seed))

    time_window_overlap_score = _time_window_overlap_score(record, scope_config.window_start, scope_config.window_end)
    time_window_ok = time_window_overlap_score > 0.0 or (scope_config.window_start is None and scope_config.window_end is None)
    include = time_window_ok and bool(
        matched_families or matched_topic_seeds or seed_semantic_similarity >= scope_config.min_seed_semantic_similarity
    )

    family_score = len(matched_families) / max(1, len(scope_config.target_families))
    seed_score = len(matched_topic_seeds) / max(1, len(scope_config.topic_seeds)) if scope_config.topic_seeds else 0.0
    total_score = clamp((0.46 * family_score) + (0.28 * seed_score) + (0.18 * seed_semantic_similarity) + (0.08 * time_window_overlap_score))
    primary_family = _resolve_primary_family(
        record,
        matched_families,
        matched_topic_seeds=matched_topic_seeds,
        seed_semantic_similarity=seed_semantic_similarity,
        min_seed_semantic_similarity=scope_config.min_seed_semantic_similarity,
    )
    return ScopeSelection(
        include=include,
        score=round(total_score, 4),
        matched_families=matched_families,
        matched_topic_seeds=matched_topic_seeds,
        seed_semantic_similarity=round(seed_semantic_similarity, 4),
        time_window_overlap_score=round(time_window_overlap_score, 4),
        primary_family=primary_family,
        family_terms=sorted(family_terms),
    )


def market_family_terms(record: MarketMetadataRecord) -> set[str]:
    families: set[str] = set()
    for part in list(record.families) + ([record.category] if record.category else []) + list(record.tags):
        normalized = normalize_text(part)
        if not normalized:
            continue
        families.add(normalized)
        for token in normalized.split(" "):
            if token:
                families.add(token)
    return families


def _resolve_primary_family(
    record: MarketMetadataRecord,
    matched_families: list[str],
    *,
    matched_topic_seeds: list[str],
    seed_semantic_similarity: float,
    min_seed_semantic_similarity: float,
) -> str | None:
    if matched_families:
        ordered_family_terms = _ordered_family_terms(record)
        for family in ordered_family_terms:
            if family in matched_families:
                return family
        return matched_families[0]
    if matched_topic_seeds or seed_semantic_similarity >= min_seed_semantic_similarity:
        return "seed_only"
    return None


def _ordered_family_terms(record: MarketMetadataRecord) -> list[str]:
    ordered_terms: list[str] = []
    seen: set[str] = set()
    for part in list(record.families) + ([record.category] if record.category else []):
        normalized = normalize_text(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered_terms.append(normalized)
    return ordered_terms


def _normalize_string_list(values: Iterable[str], *, preserve_case: bool = False) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = value.strip()
        if not text:
            continue
        key = normalize_text(text)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text if preserve_case else key)
    return normalized


def _explode_cli_list(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    items: list[str] = []
    for value in values:
        items.extend(part.strip() for part in value.split(","))
    return [item for item in items if item]


def _time_window_overlap_score(record: MarketMetadataRecord, window_start: str | None, window_end: str | None) -> float:
    if window_start is None and window_end is None:
        return 1.0
    requested_start = parse_timestamp(window_start) if window_start else None
    requested_end = parse_timestamp(window_end) if window_end else None
    market_start = parse_timestamp(record.open_time) or parse_timestamp(record.close_time) or parse_timestamp(record.resolution_time)
    market_end = parse_timestamp(record.close_time) or parse_timestamp(record.resolution_time) or market_start
    if market_start is None or market_end is None:
        return 0.0
    requested_start = requested_start or market_start
    requested_end = requested_end or market_end
    intersection_start = max(market_start, requested_start)
    intersection_end = min(market_end, requested_end)
    if intersection_end < intersection_start:
        return 0.0
    intersection = max(0.0, (intersection_end - intersection_start).total_seconds())
    union_start = min(market_start, requested_start)
    union_end = max(market_end, requested_end)
    union = max(1.0, (union_end - union_start).total_seconds())
    return intersection / union
