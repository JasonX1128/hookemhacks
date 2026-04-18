from __future__ import annotations

import argparse
from pathlib import Path

from .artifact_io import artifact_relative_path
from .common import METADATA_SCHEMA_VERSION, PipelinePaths, SCOPE_SCHEMA_VERSION
from .providers import get_provider
from .schemas import MarketMetadataRecord
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, persist_scope_artifact, resolve_scope_from_args, select_scoped_markets
from .utils import build_json_envelope, ensure_dir, read_json, update_artifact_manifest, write_json


def run(
    *,
    provider_name: str = "mock",
    scope_config: PipelineScopeConfig | None = None,
    snapshot_dir: Path | None = None,
    force: bool = False,
) -> Path:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    ensure_dir(paths.cache_dir)
    ensure_dir(paths.artifacts_dir)
    persist_scope_artifact(path=paths.scope_artifact_path, provider_name=provider_name, scope_config=scope_config)

    use_cached_scope = False
    if paths.metadata_cache_path.exists() and not force:
        cached_payload = read_json(paths.metadata_cache_path)
        use_cached_scope = cached_payload.get("scope") == scope_config.to_dict()
    if use_cached_scope:
        records = [MarketMetadataRecord.from_mapping(record) for record in cached_payload.get("records", [])]
        scope_summary = cached_payload.get("scope_summary", {})
    else:
        provider = get_provider(provider_name, snapshot_dir=snapshot_dir)
        discovered_records = sorted(provider.fetch_market_metadata(), key=lambda record: record.market_id)
        records, scope_summary = select_scoped_markets(discovered_records, scope_config)
        raw_payload = build_json_envelope(
            artifact_name="market_metadata_raw",
            provider_name=provider_name,
            schema_version=METADATA_SCHEMA_VERSION,
            record_key="records",
            records=[record.to_dict() for record in records],
            extra={
                "scope": scope_config.to_dict(),
                "scope_summary": scope_summary,
            },
        )
        write_json(paths.metadata_cache_path, raw_payload)

    artifact_payload = build_json_envelope(
        artifact_name="market_metadata",
        provider_name=provider_name,
        schema_version=METADATA_SCHEMA_VERSION,
        record_key="records",
        records=[record.to_dict() for record in records],
        extra={
            "scope": scope_config.to_dict(),
            "scope_summary": scope_summary,
            "notes": [
                "Metadata ingestion is explicitly scoped to the requested target families, topic seeds, and optional time window.",
                "Metadata powers candidate filtering before any pairwise time-series work.",
                "This artifact is intentionally lightweight and backend-loadable without re-fetching upstream data.",
            ]
        },
    )
    write_json(paths.metadata_artifact_path, artifact_payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="run_scope",
        relative_path=artifact_relative_path(paths, paths.scope_artifact_path),
        schema_version=SCOPE_SCHEMA_VERSION,
        extra={"scope_id": scope_config.scope_id},
    )
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="market_metadata",
        relative_path=artifact_relative_path(paths, paths.metadata_artifact_path),
        schema_version=METADATA_SCHEMA_VERSION,
        record_count=len(records),
        extra={"scope_id": scope_config.scope_id},
    )
    return paths.metadata_artifact_path


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
        force=args.force,
    )
    print(artifact_path)


if __name__ == "__main__":
    main()
