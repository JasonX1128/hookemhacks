from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from .artifact_io import artifact_relative_path, load_metadata_records
from .common import HISTORY_SCHEMA_VERSION, PipelinePaths
from .providers import get_provider
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, persist_scope_artifact, resolve_scope_from_args
from .utils import (
    build_json_envelope,
    ensure_dir,
    normalize_history_frame,
    read_json,
    update_artifact_manifest,
    utc_now_iso,
    write_json,
)


def _history_summary_from_frame(
    history_frame: pd.DataFrame,
    *,
    market_id: str,
    cache_status: str,
    error_message: str | None = None,
) -> dict[str, int | str | None]:
    summary: dict[str, int | str | None] = {
        "market_id": market_id,
        "rows": int(len(history_frame)),
        "first_timestamp": str(history_frame["timestamp"].min()) if not history_frame.empty else None,
        "last_timestamp": str(history_frame["timestamp"].max()) if not history_frame.empty else None,
        "cache_status": cache_status,
        "updated_at": utc_now_iso(),
    }
    if error_message:
        summary["error"] = error_message
    return summary


def _load_previous_history_manifest(paths: PipelinePaths, *, force: bool) -> tuple[dict[str, dict[str, Any]], list[str], bool]:
    if force or not paths.history_manifest_path.exists():
        return {}, [], False
    payload = read_json(paths.history_manifest_path)
    markets = payload.get("markets", [])
    if not isinstance(markets, list):
        return {}, [], False
    summaries: dict[str, dict[str, Any]] = {}
    ordered_market_ids: list[str] = []
    for item in markets:
        if not isinstance(item, dict):
            continue
        market_id = str(item.get("market_id") or "").strip()
        if not market_id:
            continue
        summaries[market_id] = dict(item)
        ordered_market_ids.append(market_id)
    return summaries, ordered_market_ids, paths.history_artifact_path.exists()


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
    previous_summaries, previous_history_order, previous_history_exists = _load_previous_history_manifest(paths, force=force)
    previous_history_market_ids = set(previous_history_order)
    target_market_ids = {market.market_id for market in metadata_records}

    reused_summaries: dict[str, dict[str, Any]] = {}
    upsert_summaries: dict[str, dict[str, Any]] = {}
    upsert_frames_by_market_id: dict[str, pd.DataFrame] = {}
    pending_markets: list = []
    cache_paths_by_market_id: dict[str, Path] = {}
    for market in metadata_records:
        cache_path = paths.history_cache_dir / f"{market.market_id}.csv"
        cache_paths_by_market_id[market.market_id] = cache_path
        should_use_cache = cache_path.exists() and not force and not provider.should_refresh_history_cache(market, cache_path)
        if should_use_cache:
            previous_summary = previous_summaries.get(market.market_id)
            if previous_summary is not None:
                reused_summaries[market.market_id] = {
                    **previous_summary,
                    "cache_status": "reused",
                }
            else:
                history_frame = normalize_history_frame(pd.read_csv(cache_path), market.market_id)
                upsert_frames_by_market_id[market.market_id] = history_frame
                upsert_summaries[market.market_id] = _history_summary_from_frame(
                    history_frame,
                    market_id=market.market_id,
                    cache_status="reused",
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
        upsert_frames_by_market_id[market.market_id] = history_frame
        upsert_summaries[market.market_id] = _history_summary_from_frame(
            history_frame,
            market_id=market.market_id,
            cache_status=cache_status,
            error_message=error_message,
        )

    removed_market_ids = previous_history_market_ids - target_market_ids
    changed_market_ids = set(upsert_frames_by_market_id)
    changed_existing_market_ids = changed_market_ids & previous_history_market_ids
    append_only_market_ids = changed_market_ids - previous_history_market_ids

    combined_history_row_count = 0
    if not force and previous_history_exists and not changed_market_ids and not removed_market_ids:
        combined_history_row_count = int(sum(int(summary.get("rows") or 0) for summary in reused_summaries.values()))
    elif (
        not force
        and previous_history_exists
        and append_only_market_ids
        and not changed_existing_market_ids
        and not removed_market_ids
    ):
        append_frames = [upsert_frames_by_market_id[market_id] for market_id in sorted(append_only_market_ids)]
        if append_frames:
            pd.concat(append_frames, ignore_index=True).to_csv(
                paths.history_artifact_path,
                mode="a",
                header=False,
                index=False,
            )
        combined_history_row_count = int(
            sum(int(summary.get("rows") or 0) for summary in reused_summaries.values())
            + sum(int(summary.get("rows") or 0) for summary in upsert_summaries.values())
        )
    else:
        if previous_history_exists and not force:
            existing_history = pd.read_csv(paths.history_artifact_path)
            if changed_market_ids or removed_market_ids:
                excluded_market_ids = changed_market_ids | removed_market_ids
                existing_history = existing_history[~existing_history["market_id"].isin(sorted(excluded_market_ids))]
        else:
            existing_history = pd.DataFrame(columns=["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"])

        frames_to_write = []
        if not existing_history.empty:
            frames_to_write.append(existing_history)
        frames_to_write.extend(
            frame for market_id, frame in sorted(upsert_frames_by_market_id.items()) if market_id in changed_market_ids and not frame.empty
        )
        combined_history = (
            pd.concat(frames_to_write, ignore_index=True)
            if frames_to_write
            else pd.DataFrame(columns=["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"])
        )
        if not combined_history.empty:
            combined_history = combined_history.sort_values(["market_id", "timestamp"])
        combined_history_row_count = int(len(combined_history))
        combined_history.to_csv(paths.history_artifact_path, index=False)

    ordered_market_ids: list[str] = []
    seen_market_ids: set[str] = set()
    for market_id in previous_history_order:
        if market_id in target_market_ids:
            ordered_market_ids.append(market_id)
            seen_market_ids.add(market_id)
    for market in metadata_records:
        if market.market_id not in seen_market_ids:
            ordered_market_ids.append(market.market_id)
            seen_market_ids.add(market.market_id)

    summaries_by_market_id: dict[str, dict[str, Any]] = {}
    summaries_by_market_id.update(reused_summaries)
    summaries_by_market_id.update(upsert_summaries)
    per_market_counts = [summaries_by_market_id[market_id] for market_id in ordered_market_ids if market_id in summaries_by_market_id]

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
                "History aggregation reuses prior manifests and only rewrites changed market slices when possible.",
            ],
        },
    )
    write_json(paths.history_manifest_path, manifest_payload)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="market_history",
        relative_path=artifact_relative_path(paths, paths.history_artifact_path),
        schema_version=HISTORY_SCHEMA_VERSION,
        record_count=combined_history_row_count,
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
