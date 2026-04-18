from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .artifact_io import artifact_relative_path
from .common import PipelinePaths, RUN_SUMMARY_SCHEMA_VERSION
from .scope import PipelineScopeConfig
from .utils import build_json_envelope, read_json, update_artifact_manifest, write_json


def write_run_summary(*, provider_name: str, scope_config: PipelineScopeConfig) -> Path:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    metadata_payload = read_json(paths.metadata_artifact_path)
    clusters_payload = read_json(paths.clusters_artifact_path)
    candidates_payload = read_json(paths.candidates_artifact_path)
    pair_features = pd.read_csv(paths.pair_features_artifact_path)
    cointegration_metrics = pd.read_csv(paths.cointegration_artifact_path)

    metadata_records = metadata_payload.get("records", [])
    candidate_records = candidates_payload.get("records", [])
    cluster_records = clusters_payload.get("clusters", [])
    category_counts = (
        pd.DataFrame(metadata_records)["category"].fillna("unknown").value_counts().sort_values(ascending=False).to_dict()
        if metadata_records
        else {}
    )
    family_counts = metadata_payload.get("scope_summary", {}).get("family_counts", {})
    candidate_frame = pd.DataFrame(candidate_records) if candidate_records else pd.DataFrame()
    cross_family_edge_count = 0
    within_family_edge_count = 0
    if not candidate_frame.empty and "cross_family_link" in candidate_frame.columns:
        cross_family_edge_count = int(candidate_frame["cross_family_link"].fillna(False).astype(bool).sum())
        within_family_edge_count = int(len(candidate_frame) - cross_family_edge_count)

    evaluated_cointegration = 0
    stationary_cointegration = 0
    if not cointegration_metrics.empty:
        if "eligible_for_test" in cointegration_metrics.columns:
            evaluated_cointegration = int(cointegration_metrics["eligible_for_test"].fillna(False).astype(bool).sum())
        if "spread_stationary_flag" in cointegration_metrics.columns:
            stationary_cointegration = int(cointegration_metrics["spread_stationary_flag"].fillna(False).astype(bool).sum())

    top_comovement_pairs: list[dict[str, Any]] = []
    if not pair_features.empty and "comovement_score" in pair_features.columns:
        top_pairs = pair_features.sort_values(
            ["comovement_score", "candidate_score"],
            ascending=[False, False],
        ).head(3)
        top_comovement_pairs = top_pairs[
            ["market_id", "related_market_id", "comovement_score", "return_correlation", "latest_residual_zscore"]
        ].to_dict(orient="records")

    top_cross_family_pairs: list[dict[str, Any]] = []
    if not pair_features.empty and "cross_family_link" in pair_features.columns:
        cross_family_pairs = pair_features[pair_features["cross_family_link"].fillna(False).astype(bool)]
        if not cross_family_pairs.empty:
            top_cross_family_pairs = cross_family_pairs.sort_values(
                ["comovement_score", "candidate_score"],
                ascending=[False, False],
            ).head(5)[
                [
                    "market_id",
                    "related_market_id",
                    "market_primary_family",
                    "candidate_primary_family",
                    "comovement_score",
                    "return_correlation",
                ]
            ].to_dict(orient="records")

    candidate_pair_sample: list[dict[str, Any]] = []
    if candidate_records:
        candidate_pair_sample = candidate_frame.sort_values(
            ["candidate_score", "semantic_similarity_score"],
            ascending=[False, False],
        ).head(5)[
            ["market_id", "candidate_market_id", "market_primary_family", "candidate_primary_family", "candidate_score", "cross_family_link"]
        ].to_dict(orient="records")

    cluster_sample = sorted(
        [
            {
                "cluster_id": cluster["cluster_id"],
                "label": cluster["label"],
                "member_count": len(cluster.get("member_market_ids", [])),
                "shared_families": cluster.get("shared_families", []),
            }
            for cluster in cluster_records
        ],
        key=lambda item: (-item["member_count"], item["label"]),
    )[:5]

    summary = {
        "scope": scope_config.to_dict(),
        "market_count": len(metadata_records),
        "category_count": len(category_counts),
        "categories": list(category_counts.keys()),
        "category_counts": category_counts,
        "family_counts": family_counts,
        "candidate_edge_count": len(candidate_records),
        "cross_family_candidate_edge_count": cross_family_edge_count,
        "within_family_candidate_edge_count": within_family_edge_count,
        "comovement_pair_count": int(len(pair_features)),
        "cointegration_evaluated_count": evaluated_cointegration,
        "cointegration_stationary_count": stationary_cointegration,
        "cluster_count": len(cluster_records),
        "clusters": cluster_sample,
        "top_comovement_pairs": top_comovement_pairs,
        "top_cross_family_pairs": top_cross_family_pairs,
        "sample_related_pairs": candidate_pair_sample,
        "notes": [
            "This summary is intended as a quick quality check for a scoped preprocessing run.",
            "Candidate and cointegration counts are based only on the selected scoped macro universe.",
        ],
    }

    payload = build_json_envelope(
        artifact_name="run_summary",
        provider_name=provider_name,
        schema_version=RUN_SUMMARY_SCHEMA_VERSION,
        record_key="records",
        records=[],
        extra={"summary": summary},
    )
    write_json(paths.run_summary_path, payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="run_summary",
        relative_path=artifact_relative_path(paths, paths.run_summary_path),
        schema_version=RUN_SUMMARY_SCHEMA_VERSION,
        extra={
            "scope_id": scope_config.scope_id,
            "market_count": len(metadata_records),
            "candidate_edge_count": len(candidate_records),
            "cointegration_evaluated_count": evaluated_cointegration,
        },
    )
    return paths.run_summary_path
