from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from .artifact_io import artifact_relative_path, load_history_series_by_market, load_metadata_records
from .common import CANDIDATE_SCHEMA_VERSION, PipelinePaths
from .schemas import MarketMetadataRecord
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, resolve_scope_from_args
from .utils import (
    build_json_envelope,
    horizon_bucket,
    safe_corr,
    semantic_similarity,
    time_overlap_score,
    tokenize_text,
    update_artifact_manifest,
    write_json,
)


def _scope_families(record: MarketMetadataRecord) -> set[str]:
    matched_families = record.extra.get("matched_scope_families", [])
    return {str(value) for value in matched_families if value} or set(record.families)


def _primary_family(record: MarketMetadataRecord) -> str | None:
    value = record.extra.get("scope_primary_family")
    return str(value) if value else None


def _cross_family_link(record: MarketMetadataRecord, candidate: MarketMetadataRecord, shared_families: list[str]) -> bool:
    left_primary = _primary_family(record)
    right_primary = _primary_family(candidate)
    if left_primary and right_primary:
        return left_primary != right_primary
    return not bool(shared_families)


def _build_indexes(
    records: list[MarketMetadataRecord],
) -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, set[str]], dict[str, set[str]], dict[str, str]]:
    tokens_by_market: dict[str, set[str]] = {}
    term_index: dict[str, set[str]] = defaultdict(set)
    family_index: dict[str, set[str]] = defaultdict(set)
    families_by_market: dict[str, set[str]] = {}
    horizon_by_market: dict[str, str] = {}

    doc_frequency: Counter[str] = Counter()
    for record in records:
        tokens = tokenize_text(record.title, record.question, record.category, " ".join(record.families), " ".join(record.tags))
        tokens_by_market[record.market_id] = tokens
        doc_frequency.update(tokens)
        families = _scope_families(record)
        families_by_market[record.market_id] = families
        for family in families:
            family_index[family].add(record.market_id)
        horizon_by_market[record.market_id] = horizon_bucket(record.open_time, record.resolution_time)

    max_term_frequency = max(2, len(records) // 2)
    for market_id, tokens in tokens_by_market.items():
        for token in tokens:
            if doc_frequency[token] <= max_term_frequency:
                term_index[token].add(market_id)

    return tokens_by_market, term_index, family_index, families_by_market, horizon_by_market


def _candidate_pool(
    *,
    record: MarketMetadataRecord,
    tokens_by_market: dict[str, set[str]],
    term_index: dict[str, set[str]],
    family_index: dict[str, set[str]],
    families_by_market: dict[str, set[str]],
    horizon_by_market: dict[str, str],
    max_pool_size: int,
) -> list[str]:
    pool: set[str] = set()
    for family in _scope_families(record):
        pool.update(family_index.get(family, set()))
    for token in tokens_by_market[record.market_id]:
        pool.update(term_index.get(token, set()))
    pool.discard(record.market_id)

    scored_pool: list[tuple[str, float]] = []
    for candidate_id in pool:
        shared_terms = tokens_by_market[record.market_id] & tokens_by_market[candidate_id]
        preliminary = float(len(shared_terms))
        if candidate_id in pool and _scope_families(record) & families_by_market.get(candidate_id, set()):
            preliminary += 2.5
        if horizon_by_market[record.market_id] == horizon_by_market.get(candidate_id):
            preliminary += 0.75
        scored_pool.append((candidate_id, preliminary))
    return [candidate_id for candidate_id, _ in sorted(scored_pool, key=lambda item: (-item[1], item[0]))[:max_pool_size]]


def _quick_corr(
    market_id: str,
    candidate_id: str,
    history_by_market: dict[str, pd.Series],
) -> tuple[float | None, int]:
    left = history_by_market.get(market_id)
    right = history_by_market.get(candidate_id)
    if left is None or right is None:
        return None, 0
    aligned_left, aligned_right = left.align(right, join="inner")
    overlap_points = int(len(aligned_left))
    if overlap_points < 4:
        return None, overlap_points
    left_values = aligned_left.to_numpy(dtype=float, copy=False)
    right_values = aligned_right.to_numpy(dtype=float, copy=False)
    left_returns = np.diff(left_values)
    right_returns = np.diff(right_values)
    if len(left_returns) < 3 or len(right_returns) < 3:
        return None, overlap_points
    return safe_corr(pd.Series(left_returns), pd.Series(right_returns)), overlap_points


def _build_clusters(
    market_records: list[MarketMetadataRecord],
    candidate_records: list[dict],
    *,
    cross_family_semantic_min: float,
) -> tuple[list[dict], dict[str, list[str]]]:
    market_by_id = {record.market_id: record for record in market_records}
    tokens_by_market = {
        record.market_id: tokenize_text(record.title, record.question, record.category, " ".join(record.families), " ".join(record.tags))
        for record in market_records
    }
    memberships: dict[str, list[str]] = defaultdict(list)
    clusters: list[dict] = []
    cluster_index = 1

    family_members: dict[str, set[str]] = defaultdict(set)
    for record in market_records:
        primary_family = _primary_family(record)
        if primary_family:
            family_members[primary_family].add(record.market_id)

    # Keep clusters scoped to primary-family groups so the macro universe produces interpretable topic clusters
    # instead of one giant connected component.
    for family_name, member_ids in sorted(family_members.items()):
        if len(member_ids) < 2:
            continue
        component = sorted(member_ids)
        cluster_id = f"cluster_{cluster_index:03d}"
        cluster_index += 1
        token_counter = Counter()
        categories = Counter()
        for member_id in component:
            token_counter.update(tokens_by_market.get(member_id, set()))
            categories[market_by_id[member_id].category or "unknown"] += 1
            memberships[member_id].append(cluster_id)
        top_terms = [token for token, _ in token_counter.most_common(5)]
        label_parts = [family_name] + top_terms[:2]
        label = " / ".join(part for part in label_parts if part)
        clusters.append(
            {
                "cluster_id": cluster_id,
                "label": label,
                "member_market_ids": component,
                "shared_categories": [category for category, _ in categories.most_common(3)],
                "shared_families": [family_name],
                "top_terms": top_terms,
            }
        )
    return clusters, memberships


def run(
    *,
    provider_name: str = "mock",
    scope_config: PipelineScopeConfig | None = None,
    top_k: int | None = None,
    max_pool_size: int | None = None,
) -> tuple[Path, Path]:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    top_k = top_k if top_k is not None else scope_config.top_k
    max_pool_size = max_pool_size if max_pool_size is not None else scope_config.max_pool_size
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    metadata_records = load_metadata_records(paths)
    history_by_market = load_history_series_by_market(paths)
    record_by_id = {record.market_id: record for record in metadata_records}
    tokens_by_market, term_index, family_index, families_by_market, horizon_by_market = _build_indexes(metadata_records)

    candidate_records: list[dict] = []
    for record in metadata_records:
        pool = _candidate_pool(
            record=record,
            tokens_by_market=tokens_by_market,
            term_index=term_index,
            family_index=family_index,
            families_by_market=families_by_market,
            horizon_by_market=horizon_by_market,
            max_pool_size=max_pool_size,
        )
        scored_candidates: list[dict] = []
        for candidate_id in pool:
            candidate = record_by_id[candidate_id]
            shared_terms = sorted(tokens_by_market[record.market_id] & tokens_by_market[candidate_id])
            shared_tags = sorted(set(record.tags) & set(candidate.tags))
            shared_families = sorted(_scope_families(record) & _scope_families(candidate))
            cross_family_link = _cross_family_link(record, candidate, shared_families)
            semantic_score = semantic_similarity(record.combined_text, candidate.combined_text)
            same_primary_family = not cross_family_link
            if same_primary_family:
                family_alignment_score = 1.0
            elif shared_families:
                family_alignment_score = 0.6
            else:
                family_alignment_score = 0.0
            if record.category and record.category == candidate.category:
                category_overlap_score = 1.0
            else:
                category_overlap_score = min(0.7, (0.18 * len(shared_tags)) + (0.08 * len(shared_terms)))
            time_score = time_overlap_score(
                record.open_time,
                record.close_time,
                record.resolution_time,
                candidate.open_time,
                candidate.close_time,
                candidate.resolution_time,
            )
            quick_corr, overlap_points = _quick_corr(record.market_id, candidate_id, history_by_market)
            quick_comovement = abs(quick_corr) if quick_corr is not None else 0.0
            cross_family_bridge = len(shared_families) >= 2 or len(shared_tags) >= 1 or len(shared_terms) >= 2
            cross_family_supported = semantic_score >= scope_config.cross_family_semantic_min or (
                bool(shared_families)
                and cross_family_bridge
                and time_score >= 0.3
                and quick_comovement >= 0.28
            )
            if cross_family_link and not cross_family_supported:
                continue
            candidate_score = round(
                (0.26 * family_alignment_score)
                + (0.22 * category_overlap_score)
                + (0.30 * semantic_score)
                + (0.14 * time_score)
                + (0.08 * quick_comovement),
                4,
            )
            scored_candidates.append(
                {
                    "market_id": record.market_id,
                    "candidate_market_id": candidate_id,
                    "candidate_title": candidate.title,
                    "candidate_category": candidate.category,
                    "market_primary_family": _primary_family(record),
                    "candidate_primary_family": _primary_family(candidate),
                    "shared_families": shared_families,
                    "cross_family_link": cross_family_link,
                    "family_alignment_score": round(float(family_alignment_score), 4),
                    "category_overlap_score": round(float(category_overlap_score), 4),
                    "semantic_similarity_score": round(float(semantic_score), 4),
                    "time_horizon_overlap_score": round(float(time_score), 4),
                    "quick_return_correlation": round(float(quick_corr), 4) if quick_corr is not None else None,
                    "quick_comovement_score": round(float(quick_comovement), 4),
                    "cross_family_supported": bool(cross_family_supported),
                    "overlapping_history_points": int(overlap_points),
                    "candidate_score": candidate_score,
                    "shared_terms": shared_terms[:8],
                    "shared_tags": shared_tags,
                    "shared_horizon_bucket": horizon_by_market[record.market_id] == horizon_by_market[candidate_id],
                    "cluster_ids": [],
                    "notes": (
                        "Within-primary-family scoped link."
                        if not cross_family_link
                        else "Cross-primary-family link admitted only after strong semantic or bridge evidence inside the scoped macro universe."
                    ),
                }
            )

        filtered_candidates: list[dict] = []
        for item in scored_candidates:
            if item["cross_family_link"]:
                keep = bool(item.get("cross_family_supported")) and (
                    item["candidate_score"] >= 0.34 or item["quick_comovement_score"] >= 0.28
                )
            else:
                keep = (
                    item["candidate_score"] >= 0.22
                    or item["category_overlap_score"] >= 0.45
                    or item["semantic_similarity_score"] >= 0.42
                )
            if keep:
                filtered_candidates.append(item)

        sorted_candidates = sorted(filtered_candidates, key=lambda item: (-item["candidate_score"], item["candidate_market_id"]))
        primary_family_candidates = [item for item in sorted_candidates if not item["cross_family_link"]]
        cross_family_candidates = [item for item in sorted_candidates if item["cross_family_link"]]
        cross_family_slots = 1 if top_k >= 5 and cross_family_candidates else 0

        selected_candidates: list[dict] = []
        selected_ids: set[str] = set()
        for item in primary_family_candidates[: max(0, top_k - cross_family_slots)]:
            selected_candidates.append(item)
            selected_ids.add(item["candidate_market_id"])
        for item in cross_family_candidates[:cross_family_slots]:
            if item["candidate_market_id"] in selected_ids:
                continue
            selected_candidates.append(item)
            selected_ids.add(item["candidate_market_id"])
        for item in sorted_candidates:
            if len(selected_candidates) >= top_k:
                break
            if item["candidate_market_id"] in selected_ids:
                continue
            selected_candidates.append(item)
            selected_ids.add(item["candidate_market_id"])

        for rank, candidate_record in enumerate(
            sorted(selected_candidates, key=lambda item: (-item["candidate_score"], item["candidate_market_id"]))[:top_k],
            start=1,
        ):
            candidate_record["rank"] = rank
            candidate_records.append(candidate_record)

    clusters, memberships = _build_clusters(
        metadata_records,
        candidate_records,
        cross_family_semantic_min=scope_config.cross_family_semantic_min,
    )
    for candidate in candidate_records:
        shared_clusters = sorted(
            set(memberships.get(candidate["market_id"], [])) & set(memberships.get(candidate["candidate_market_id"], []))
        )
        candidate["cluster_ids"] = shared_clusters

    cluster_payload = build_json_envelope(
        artifact_name="market_clusters",
        provider_name=provider_name,
        schema_version=CANDIDATE_SCHEMA_VERSION,
        record_key="clusters",
        records=clusters,
        extra={
            "scope": scope_config.to_dict(),
            "market_memberships": [
                {"market_id": market_id, "cluster_ids": cluster_ids}
                for market_id, cluster_ids in sorted(memberships.items())
            ],
            "notes": [
                "Clusters are built only within the scoped market universe selected at ingest time.",
                "Cross-family edges only survive when semantic similarity is strong enough to justify them.",
            ],
        },
    )
    write_json(paths.clusters_artifact_path, cluster_payload)

    candidate_payload = build_json_envelope(
        artifact_name="related_candidates",
        provider_name=provider_name,
        schema_version=CANDIDATE_SCHEMA_VERSION,
        record_key="records",
        records=sorted(candidate_records, key=lambda item: (item["market_id"], item["rank"], item["candidate_market_id"])),
        extra={
            "scope": scope_config.to_dict(),
            "top_k_per_market": top_k,
            "selection_rules": [
                "Candidate pools are seeded from scoped families and informative term overlap rather than all-market brute force.",
                "Within-family links are preferred; cross-family links require strong semantic similarity.",
            ],
        },
    )
    write_json(paths.candidates_artifact_path, candidate_payload)

    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="market_clusters",
        relative_path=artifact_relative_path(paths, paths.clusters_artifact_path),
        schema_version=CANDIDATE_SCHEMA_VERSION,
        record_count=len(clusters),
        extra={"scope_id": scope_config.scope_id},
    )
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="related_candidates",
        relative_path=artifact_relative_path(paths, paths.candidates_artifact_path),
        schema_version=CANDIDATE_SCHEMA_VERSION,
        record_count=len(candidate_records),
        extra={"scope_id": scope_config.scope_id},
    )
    return paths.clusters_artifact_path, paths.candidates_artifact_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build plausible related-market candidates from cached scoped artifacts.")
    add_scope_arguments(parser)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    provider_name, scope_config = resolve_scope_from_args(args)
    cluster_path, candidate_path = run(
        provider_name=provider_name,
        scope_config=scope_config,
        top_k=args.top_k,
        max_pool_size=args.max_pool_size,
    )
    print(cluster_path)
    print(candidate_path)


if __name__ == "__main__":
    main()
