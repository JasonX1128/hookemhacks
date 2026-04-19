from __future__ import annotations

import argparse
from pathlib import Path
import time

from .artifact_io import artifact_relative_path
from .common import (
    METADATA_SCHEMA_VERSION,
    PIPELINE_PROGRESS_SCHEMA_VERSION,
    PipelinePaths,
    RELATED_MARKETS_UNIVERSE_SCHEMA_VERSION,
    SCOPE_SCHEMA_VERSION,
)
from .market_state import merge_market_records, prune_concluded_market_records
from .manual_categorization import load_app_enabled_market_records
from .publishing import publish_metadata_snapshot
from .providers import get_provider
from .schemas import MarketMetadataRecord
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, persist_scope_artifact, resolve_scope_from_args, select_scoped_markets
from .utils import build_json_envelope, ensure_dir, read_json, update_artifact_manifest, write_json


def _build_related_markets_universe_records(records: list[MarketMetadataRecord]) -> list[dict[str, object]]:
    universe: list[dict[str, object]] = []
    for record in records:
        scope_score = 0.0
        try:
            scope_score = float(record.extra.get("scope_score") or 0.0)
        except (TypeError, ValueError):
            scope_score = 0.0
        category_score = max(0.25, min(1.0, scope_score))
        semantic_boost = max(0.0, min(1.0, scope_score + 0.08))
        historical_comovement = 0.15
        expected_reaction = max(0.18, 0.5 * category_score + 0.5 * historical_comovement)
        universe.append(
            {
                "marketId": record.market_id,
                "title": record.title,
                "question": record.question,
                "category": record.category,
                "families": list(record.families),
                "tags": list(record.tags),
                "eventTicker": str(record.extra.get("event_ticker") or "") or None,
                "seriesTicker": str(record.extra.get("series_ticker") or "") or None,
                "status": record.status,
                "closeTime": record.close_time,
                "resolutionTime": record.resolution_time,
                "categoryScore": round(category_score, 4),
                "semanticBoost": round(semantic_boost, 4),
                "historicalComovement": historical_comovement,
                "expectedReactionScore": round(expected_reaction, 4),
                "residualZscore": 0.0,
                "proxyType": None,
                "note": "Worth checking: metadata-derived related market candidate from the latest live refresh.",
                "enoughHistory": False,
            }
        )
    return universe


def run(
    *,
    provider_name: str = "mock",
    scope_config: PipelineScopeConfig | None = None,
    snapshot_dir: Path | None = None,
    config_path: Path | None = None,
    force: bool = False,
    incremental_snapshots: bool = False,
    snapshot_interval_seconds: float = 1.0,
) -> Path:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    ensure_dir(paths.cache_dir)
    ensure_dir(paths.artifacts_dir)
    persist_scope_artifact(path=paths.scope_artifact_path, provider_name=provider_name, scope_config=scope_config)

    provider = get_provider(provider_name, snapshot_dir=snapshot_dir, config_path=config_path)
    provider.set_metadata_progress_path(paths.pipeline_progress_path)
    last_snapshot_write_at = 0.0
    accumulated_records = _load_existing_metadata_records(paths)
    preserve_existing_artifact_records = False

    def coalesce_snapshot_records(
        snapshot_records: list[MarketMetadataRecord],
        *,
        status: str,
    ) -> list[MarketMetadataRecord]:
        nonlocal accumulated_records
        effective_records = sorted(snapshot_records, key=lambda record: record.market_id)
        if not preserve_existing_artifact_records:
            return effective_records
        effective_records = merge_market_records(accumulated_records, effective_records)
        if status == "completed":
            effective_records = prune_concluded_market_records(effective_records)
        accumulated_records = effective_records
        return effective_records

    def coalesced_scope_summary(scope_summary: dict, records: list[MarketMetadataRecord]) -> dict:
        summary = dict(scope_summary)
        summary["selected_market_count"] = len(records)
        summary["selected_market_ids"] = [record.market_id for record in records]
        return summary

    def write_metadata_snapshot(
        snapshot_records: list[MarketMetadataRecord],
        scope_summary: dict,
        *,
        discovery_source: str,
        status: str,
        message: str,
        coalesce_records: bool = True,
    ) -> None:
        effective_records = (
            coalesce_snapshot_records(snapshot_records, status=status)
            if coalesce_records
            else sorted(snapshot_records, key=lambda record: record.market_id)
        )
        effective_scope_summary = coalesced_scope_summary(scope_summary, effective_records)
        artifact_payload = build_json_envelope(
            artifact_name="market_metadata",
            provider_name=provider_name,
            schema_version=METADATA_SCHEMA_VERSION,
            record_key="records",
            records=[record.to_dict() for record in effective_records],
            extra={
                "scope": scope_config.to_dict(),
                "scope_summary": effective_scope_summary,
                "discovery_source": discovery_source,
                "status": status,
                "notes": [
                    "Metadata ingestion is explicitly scoped to the requested target families and topic seeds unless both are empty, in which case local filtering is disabled.",
                    "When manual categorization artifacts exist, only promoted app-enabled markets are eligible for this artifact.",
                    "Metadata powers candidate filtering before any pairwise time-series work.",
                    "This artifact is intentionally lightweight and backend-loadable without re-fetching upstream data.",
                ],
            },
        )
        universe_payload = build_json_envelope(
            artifact_name="related_markets_universe",
            provider_name=provider_name,
            schema_version=RELATED_MARKETS_UNIVERSE_SCHEMA_VERSION,
            record_key="records",
            records=_build_related_markets_universe_records(effective_records),
            extra={
                "scope": scope_config.to_dict(),
                "discovery_source": discovery_source,
                "status": status,
                "notes": [
                    "This reduced artifact exists for fast backend related-market lookups.",
                    "It intentionally omits most raw metadata fields that are not needed for related-market retrieval.",
                ],
            },
        )
        write_json(paths.metadata_artifact_path, artifact_payload)
        write_json(paths.related_markets_universe_path, universe_payload)
        write_json(
            paths.pipeline_progress_path,
            {
                "artifact": "pipeline_progress",
                "provider": provider_name,
                "schema_version": PIPELINE_PROGRESS_SCHEMA_VERSION,
                "generated_at": artifact_payload["generated_at"],
                "status": status,
                "discovered_market_count": len(snapshot_records),
                "artifact_market_count": len(effective_records),
                "message": message,
            },
        )
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="market_metadata",
            relative_path=artifact_relative_path(paths, paths.metadata_artifact_path),
            schema_version=METADATA_SCHEMA_VERSION,
            record_count=len(effective_records),
            extra={"scope_id": scope_config.scope_id},
        )
        update_artifact_manifest(
            manifest_path=paths.artifact_manifest_path,
            artifact_key="related_markets_universe",
            relative_path=artifact_relative_path(paths, paths.related_markets_universe_path),
            schema_version=RELATED_MARKETS_UNIVERSE_SCHEMA_VERSION,
            record_count=len(effective_records),
            extra={"scope_id": scope_config.scope_id},
        )

    def handle_incremental_snapshot(snapshot_records: list[MarketMetadataRecord], stage_label: str) -> None:
        nonlocal last_snapshot_write_at
        if not incremental_snapshots:
            return
        now = time.time()
        if now - last_snapshot_write_at < max(0.0, snapshot_interval_seconds):
            return
        scope_records, scope_summary = select_scoped_markets(snapshot_records, scope_config)
        write_metadata_snapshot(
            scope_records,
            scope_summary,
            discovery_source="provider_incremental_snapshot",
            status="running",
            message=f"Incremental metadata snapshot written during {stage_label}.",
        )
        last_snapshot_write_at = now

    provider.set_metadata_snapshot_callback(handle_incremental_snapshot)
    use_cached_scope = False
    if paths.metadata_cache_path.exists() and not force:
        cached_payload = read_json(paths.metadata_cache_path)
        use_cached_scope = cached_payload.get("scope") == scope_config.to_dict()
        if use_cached_scope:
            use_cached_scope = not provider.should_refresh_metadata_cache(
                paths.metadata_cache_path,
                scope_config=scope_config,
            )
    if use_cached_scope:
        records = [MarketMetadataRecord.from_mapping(record) for record in cached_payload.get("records", [])]
        scope_summary = cached_payload.get("scope_summary", {})
        provider.mark_metadata_progress(
            status="completed",
            discovered_market_count=len(records),
            message="Using cached scoped metadata artifact.",
        )
        write_metadata_snapshot(
            records,
            scope_summary,
            discovery_source=str(cached_payload.get("discovery_source") or "cache"),
            status="completed",
            message="Using cached scoped metadata artifact.",
        )
    else:
        provider.mark_metadata_progress(status="running", discovered_market_count=0, message="Starting market metadata discovery.")
        discovered_records = load_app_enabled_market_records(paths)
        discovery_source = "manual_categorization_state"
        if not discovered_records:
            preserve_existing_artifact_records = True
            discovery_mode = "scoped" if scope_config.has_local_filters else "all"
            discovered_records = sorted(
                provider.fetch_market_metadata(scope_config=scope_config, discovery_mode=discovery_mode),
                key=lambda record: record.market_id,
            )
            discovery_source = "provider_legacy_fallback"
        records, scope_summary = select_scoped_markets(discovered_records, scope_config)
        if preserve_existing_artifact_records:
            records = coalesce_snapshot_records(records, status="completed")
            scope_summary = coalesced_scope_summary(scope_summary, records)
        raw_payload = build_json_envelope(
            artifact_name="market_metadata_raw",
            provider_name=provider_name,
            schema_version=METADATA_SCHEMA_VERSION,
            record_key="records",
            records=[record.to_dict() for record in records],
            extra={
                "scope": scope_config.to_dict(),
                "scope_summary": scope_summary,
                "discovery_source": discovery_source,
            },
        )
        write_json(paths.metadata_cache_path, raw_payload)
        provider.mark_metadata_progress(
            status="running",
            discovered_market_count=len(records),
            message="Scoped market selection completed; writing metadata artifact.",
        )

    write_metadata_snapshot(
        records,
        scope_summary,
        discovery_source=discovery_source if not use_cached_scope else "cache",
        status="completed",
        message="Market metadata artifact written successfully.",
        coalesce_records=not preserve_existing_artifact_records,
    )
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="run_scope",
        relative_path=artifact_relative_path(paths, paths.scope_artifact_path),
        schema_version=SCOPE_SCHEMA_VERSION,
        extra={"scope_id": scope_config.scope_id},
    )
    publish_metadata_snapshot(paths)
    return paths.metadata_artifact_path


def _load_existing_metadata_records(paths: PipelinePaths) -> list[MarketMetadataRecord]:
    if not paths.metadata_artifact_path.exists():
        return []
    try:
        payload = read_json(paths.metadata_artifact_path)
    except Exception:
        return []
    records = payload.get("records", [])
    return [
        MarketMetadataRecord.from_mapping(record)
        for record in records
        if isinstance(record, dict) and record.get("market_id")
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch or load market metadata into local cache/artifacts.")
    add_scope_arguments(parser)
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=None,
        help="Optional directory containing snapshot files when using the snapshot provider.",
    )
    parser.add_argument("--force", action="store_true", help="Refresh the cache even if files already exist.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    provider_name, scope_config = resolve_scope_from_args(args)
    artifact_path = run(
        provider_name=provider_name,
        scope_config=scope_config,
        snapshot_dir=args.snapshot_dir,
        config_path=args.config,
        force=args.force,
    )
    print(artifact_path)


if __name__ == "__main__":
    main()
