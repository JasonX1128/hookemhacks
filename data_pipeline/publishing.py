from __future__ import annotations

from pathlib import Path

from .artifact_io import artifact_relative_path
from .common import PipelinePaths
from .utils import copy_file_atomic, ensure_dir, read_json, update_artifact_manifest


def _publish_one(
    *,
    paths: PipelinePaths,
    source: Path,
    destination: Path,
    artifact_key: str,
) -> None:
    if not source.exists():
        return
    copy_file_atomic(source, destination)

    source_manifest = read_json(paths.artifact_manifest_path) if paths.artifact_manifest_path.exists() else {}
    source_entry = source_manifest.get("artifacts", {}).get(artifact_key, {})
    schema_version = str(source_entry.get("schema_version") or "1.0")
    record_count = source_entry.get("record_count")
    extra = {key: value for key, value in source_entry.items() if key not in {"path", "schema_version", "updated_at", "record_count"}}
    update_artifact_manifest(
        manifest_path=paths.published_artifact_manifest_path,
        artifact_key=artifact_key,
        relative_path=artifact_relative_path(paths, destination),
        schema_version=schema_version,
        record_count=record_count if isinstance(record_count, int) else None,
        extra=extra or None,
    )


def publish_metadata_snapshot(paths: PipelinePaths) -> None:
    ensure_dir(paths.published_dir)
    _publish_one(
        paths=paths,
        source=paths.scope_artifact_path,
        destination=paths.published_scope_artifact_path,
        artifact_key="run_scope",
    )
    _publish_one(
        paths=paths,
        source=paths.metadata_artifact_path,
        destination=paths.published_metadata_artifact_path,
        artifact_key="market_metadata",
    )
    _publish_one(
        paths=paths,
        source=paths.related_markets_universe_path,
        destination=paths.published_related_markets_universe_path,
        artifact_key="related_markets_universe",
    )


def publish_related_markets_snapshot(paths: PipelinePaths) -> None:
    ensure_dir(paths.published_dir)
    publish_metadata_snapshot(paths)
    _publish_one(
        paths=paths,
        source=paths.pair_features_artifact_path,
        destination=paths.published_pair_features_artifact_path,
        artifact_key="pair_features",
    )
    _publish_one(
        paths=paths,
        source=paths.cointegration_artifact_path,
        destination=paths.published_cointegration_artifact_path,
        artifact_key="cointegration_metrics",
    )
    _publish_one(
        paths=paths,
        source=paths.run_summary_path,
        destination=paths.published_run_summary_path,
        artifact_key="run_summary",
    )
