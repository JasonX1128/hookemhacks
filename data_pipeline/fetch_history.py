from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .artifact_io import artifact_relative_path, load_metadata_records
from .common import HISTORY_SCHEMA_VERSION, PipelinePaths
from .providers import get_provider
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, persist_scope_artifact, resolve_scope_from_args
from .utils import build_json_envelope, ensure_dir, normalize_history_frame, update_artifact_manifest, utc_now_iso, write_json


def run(
    *,
    provider_name: str = "mock",
    scope_config: PipelineScopeConfig | None = None,
    snapshot_dir: Path | None = None,
    config_path: Path | None = None,
    force: bool = False,
) -> Path:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    ensure_dir(paths.history_cache_dir)
    ensure_dir(paths.artifacts_dir)
    persist_scope_artifact(path=paths.scope_artifact_path, provider_name=provider_name, scope_config=scope_config)

    metadata_records = load_metadata_records(paths)
    provider = get_provider(provider_name, snapshot_dir=snapshot_dir, config_path=config_path)

    cached_frames: list[pd.DataFrame] = []
    per_market_counts: list[dict[str, int | str]] = []
    pending_markets: list = []
    cache_paths_by_market_id: dict[str, Path] = {}
    for market in metadata_records:
        cache_path = paths.history_cache_dir / f"{market.market_id}.csv"
        cache_paths_by_market_id[market.market_id] = cache_path
        should_use_cache = cache_path.exists() and not force and not provider.should_refresh_history_cache(market, cache_path)
        if should_use_cache:
            history_frame = pd.read_csv(cache_path)
            history_frame = normalize_history_frame(history_frame, market.market_id)
            cached_frames.append(history_frame)
            per_market_counts.append(
                {
                    "market_id": market.market_id,
                    "rows": int(len(history_frame)),
                    "first_timestamp": str(history_frame["timestamp"].min()) if not history_frame.empty else None,
                    "last_timestamp": str(history_frame["timestamp"].max()) if not history_frame.empty else None,
                    "cache_status": "reused",
                }
            )
        else:
            pending_markets.append(market)

    fetched_histories: dict[str, pd.DataFrame] = {}
    if pending_markets:
        try:
            fetched_histories = provider.fetch_market_histories(pending_markets)
        except Exception:
            fetched_histories = {}

    for market in pending_markets:
        cache_path = cache_paths_by_market_id[market.market_id]
        error_message: str | None = None
        try:
            fetched = fetched_histories.get(market.market_id)
            if fetched is None:
                fetched = provider.fetch_market_history(market)
            history_frame = normalize_history_frame(fetched, market.market_id)
            history_frame.to_csv(cache_path, index=False)
            cache_status = "refreshed"
        except Exception as exc:
            error_message = str(exc)
            if cache_path.exists():
                history_frame = normalize_history_frame(pd.read_csv(cache_path), market.market_id)
                cache_status = "stale_cache_fallback"
            else:
                history_frame = pd.DataFrame(
                    columns=["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"]
                )
                cache_status = "fetch_failed_empty"
        cached_frames.append(history_frame)
        market_summary: dict[str, int | str | None] = {
            "market_id": market.market_id,
            "rows": int(len(history_frame)),
            "first_timestamp": str(history_frame["timestamp"].min()) if not history_frame.empty else None,
            "last_timestamp": str(history_frame["timestamp"].max()) if not history_frame.empty else None,
            "cache_status": cache_status,
            "updated_at": utc_now_iso(),
        }
        if error_message:
            market_summary["error"] = error_message
        per_market_counts.append(market_summary)

    if cached_frames:
        combined_history = pd.concat(cached_frames, ignore_index=True).sort_values(["market_id", "timestamp"])
    else:
        combined_history = pd.DataFrame(columns=["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"])
    combined_history.to_csv(paths.history_artifact_path, index=False)

    manifest_payload = build_json_envelope(
        artifact_name="history_manifest",
        provider_name=provider_name,
        schema_version=HISTORY_SCHEMA_VERSION,
        record_key="markets",
        records=per_market_counts,
        extra={
            "scope": scope_config.to_dict(),
            "history_artifact": artifact_relative_path(paths, paths.history_artifact_path),
            "columns": list(combined_history.columns),
            "notes": [
                "Historical fetching only runs for the scoped market set selected during metadata ingestion.",
                "Normalized candle rows use UTC timestamps and bounded 0..1 prices.",
                "Per-market CSV cache files allow incremental refreshes without re-fetching the full universe.",
            ],
        },
    )
    write_json(paths.history_manifest_path, manifest_payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="market_history",
        relative_path=artifact_relative_path(paths, paths.history_artifact_path),
        schema_version=HISTORY_SCHEMA_VERSION,
        record_count=int(len(combined_history)),
        extra={"manifest_path": artifact_relative_path(paths, paths.history_manifest_path), "scope_id": scope_config.scope_id},
    )
    return paths.history_artifact_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch or load market history into cache/artifacts.")
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
