from __future__ import annotations

import argparse
import json
from pathlib import Path
import threading
import time

from . import build_candidates, compute_cointegration, compute_comovement, fetch_history, fetch_markets
from .common import PipelinePaths
from .reporting import write_run_summary
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, resolve_scope_from_args
from .utils import read_json


def _load_runtime_orchestration(config_path: Path | None) -> dict[str, object]:
    if config_path is None or not config_path.exists():
        return {}
    payload = read_json(config_path)
    runtime = payload.get("runtime_orchestration")
    return runtime if isinstance(runtime, dict) else {}


def _metadata_record_count(paths: PipelinePaths) -> int:
    if paths.artifact_manifest_path.exists():
        manifest = read_json(paths.artifact_manifest_path)
        artifact_entry = manifest.get("artifacts", {}).get("market_metadata", {})
        record_count = artifact_entry.get("record_count")
        if isinstance(record_count, int):
            return record_count
    if paths.pipeline_progress_path.exists():
        payload = read_json(paths.pipeline_progress_path)
        record_count = payload.get("artifact_market_count")
        if isinstance(record_count, int):
            return record_count
    return 0


def _run_downstream_stages(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    top_k: int | None,
    max_pool_size: int | None,
    min_overlap: int,
    min_candidate_score: float,
    min_abs_return_corr: float,
    min_markets_for_downstream: int,
) -> list[Path]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    if _metadata_record_count(paths) < min_markets_for_downstream:
        return []
    outputs = list(build_candidates.run(provider_name=provider_name, scope_config=scope_config, top_k=top_k, max_pool_size=max_pool_size))
    outputs.append(compute_comovement.run(provider_name=provider_name, scope_config=scope_config))
    outputs.append(
        compute_cointegration.run(
            provider_name=provider_name,
            scope_config=scope_config,
            min_overlap=min_overlap,
            min_candidate_score=min_candidate_score,
            min_abs_return_corr=min_abs_return_corr,
        )
    )
    outputs.append(write_run_summary(provider_name=provider_name, scope_config=scope_config))
    return outputs


def _run_incremental_all(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig,
    snapshot_dir: Path | None,
    config_path: Path | None,
    force: bool,
    top_k: int | None,
    max_pool_size: int | None,
    min_overlap: int,
    min_candidate_score: float,
    min_abs_return_corr: float,
    metadata_snapshot_interval_seconds: float,
    history_poll_interval_seconds: float,
    downstream_rebuild_interval_seconds: float,
    min_markets_for_downstream: int,
) -> list[Path]:
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    outputs_by_path: dict[Path, None] = {}
    metadata_error: Exception | None = None

    def record_outputs(stage_outputs: list[Path]) -> None:
        for output in stage_outputs:
            outputs_by_path[output] = None

    def metadata_worker() -> None:
        nonlocal metadata_error
        try:
            record_outputs(
                [
                    fetch_markets.run(
                        provider_name=provider_name,
                        scope_config=scope_config,
                        snapshot_dir=snapshot_dir,
                        config_path=config_path,
                        force=force,
                        incremental_snapshots=True,
                        snapshot_interval_seconds=metadata_snapshot_interval_seconds,
                    )
                ]
            )
        except Exception as exc:  # pragma: no cover - surfaced after join
            metadata_error = exc

    thread = threading.Thread(target=metadata_worker, name="pipeline-metadata-worker", daemon=True)
    thread.start()

    last_metadata_signature: int | None = None
    last_history_run_at = 0.0
    last_downstream_run_at = 0.0

    while thread.is_alive():
        metadata_signature = paths.metadata_artifact_path.stat().st_mtime_ns if paths.metadata_artifact_path.exists() else None
        now = time.time()
        if metadata_signature is not None and (
            metadata_signature != last_metadata_signature or now - last_history_run_at >= history_poll_interval_seconds
        ):
            record_outputs(
                [
                    fetch_history.run(
                        provider_name=provider_name,
                        scope_config=scope_config,
                        snapshot_dir=snapshot_dir,
                        config_path=config_path,
                        force=False,
                    )
                ]
            )
            last_metadata_signature = metadata_signature
            last_history_run_at = now

            if now - last_downstream_run_at >= downstream_rebuild_interval_seconds:
                record_outputs(
                    _run_downstream_stages(
                        provider_name=provider_name,
                        scope_config=scope_config,
                        top_k=top_k,
                        max_pool_size=max_pool_size,
                        min_overlap=min_overlap,
                        min_candidate_score=min_candidate_score,
                        min_abs_return_corr=min_abs_return_corr,
                        min_markets_for_downstream=min_markets_for_downstream,
                    )
                )
                last_downstream_run_at = now
        time.sleep(max(0.5, min(history_poll_interval_seconds, 5.0)))

    thread.join()
    if metadata_error is not None:
        raise metadata_error

    record_outputs(
        [
            fetch_history.run(
                provider_name=provider_name,
                scope_config=scope_config,
                snapshot_dir=snapshot_dir,
                config_path=config_path,
                force=False,
            )
        ]
    )
    record_outputs(
        _run_downstream_stages(
            provider_name=provider_name,
            scope_config=scope_config,
            top_k=top_k,
            max_pool_size=max_pool_size,
            min_overlap=min_overlap,
            min_candidate_score=min_candidate_score,
            min_abs_return_corr=min_abs_return_corr,
            min_markets_for_downstream=min_markets_for_downstream,
        )
    )
    return list(outputs_by_path.keys())


def run_all(
    *,
    provider_name: str,
    scope_config: PipelineScopeConfig | None = None,
    snapshot_dir: Path | None = None,
    config_path: Path | None = None,
    force: bool = False,
    top_k: int | None = None,
    max_pool_size: int | None = None,
    min_overlap: int = 30,
    min_candidate_score: float = 0.55,
    min_abs_return_corr: float = 0.25,
) -> list[Path]:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    runtime = _load_runtime_orchestration(config_path)
    if bool(runtime.get("incremental_parallel_pipeline", False)):
        return _run_incremental_all(
            provider_name=provider_name,
            scope_config=scope_config,
            snapshot_dir=snapshot_dir,
            config_path=config_path,
            force=force,
            top_k=top_k,
            max_pool_size=max_pool_size,
            min_overlap=min_overlap,
            min_candidate_score=min_candidate_score,
            min_abs_return_corr=min_abs_return_corr,
            metadata_snapshot_interval_seconds=float(runtime.get("metadata_snapshot_interval_seconds", 1.0)),
            history_poll_interval_seconds=float(runtime.get("history_poll_interval_seconds", 5.0)),
            downstream_rebuild_interval_seconds=float(runtime.get("downstream_rebuild_interval_seconds", 30.0)),
            min_markets_for_downstream=int(runtime.get("min_markets_for_downstream", 10)),
        )
    outputs = [
        fetch_markets.run(
            provider_name=provider_name,
            scope_config=scope_config,
            snapshot_dir=snapshot_dir,
            config_path=config_path,
            force=force,
        ),
        fetch_history.run(
            provider_name=provider_name,
            scope_config=scope_config,
            snapshot_dir=snapshot_dir,
            config_path=config_path,
            force=force,
        ),
    ]
    outputs.extend(build_candidates.run(provider_name=provider_name, scope_config=scope_config, top_k=top_k, max_pool_size=max_pool_size))
    outputs.append(compute_comovement.run(provider_name=provider_name, scope_config=scope_config))
    outputs.append(
        compute_cointegration.run(
            provider_name=provider_name,
            scope_config=scope_config,
            min_overlap=min_overlap,
            min_candidate_score=min_candidate_score,
            min_abs_return_corr=min_abs_return_corr,
        )
    )
    outputs.append(write_run_summary(provider_name=provider_name, scope_config=scope_config))
    return outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one or more preprocessing stages for related-market analysis.")
    subparsers = parser.add_subparsers(dest="stage", required=True)

    stage_names = ["markets", "history", "candidates", "comovement", "cointegration", "all"]
    for name in stage_names:
        subparser = subparsers.add_parser(name)
        add_scope_arguments(subparser)
        subparser.add_argument("--snapshot-dir", type=Path, default=None)
        if name in {"markets", "history", "all"}:
            subparser.add_argument("--force", action="store_true")
        if name in {"candidates", "all"}:
            pass
        if name in {"cointegration", "all"}:
            subparser.add_argument("--min-overlap", type=int, default=30)
            subparser.add_argument("--min-candidate-score", type=float, default=0.55)
            subparser.add_argument("--min-abs-return-corr", type=float, default=0.25)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    provider_name, scope_config = resolve_scope_from_args(args)
    if args.stage == "markets":
        outputs = [
            fetch_markets.run(
                provider_name=provider_name,
                scope_config=scope_config,
                snapshot_dir=args.snapshot_dir,
                config_path=args.config,
                force=args.force,
            )
        ]
    elif args.stage == "history":
        outputs = [
            fetch_history.run(
                provider_name=provider_name,
                scope_config=scope_config,
                snapshot_dir=args.snapshot_dir,
                config_path=args.config,
                force=args.force,
            )
        ]
    elif args.stage == "candidates":
        outputs = list(build_candidates.run(provider_name=provider_name, scope_config=scope_config, top_k=args.top_k, max_pool_size=args.max_pool_size))
    elif args.stage == "comovement":
        outputs = [compute_comovement.run(provider_name=provider_name, scope_config=scope_config)]
    elif args.stage == "cointegration":
        outputs = [
            compute_cointegration.run(
                provider_name=provider_name,
                scope_config=scope_config,
                min_overlap=args.min_overlap,
                min_candidate_score=args.min_candidate_score,
                min_abs_return_corr=args.min_abs_return_corr,
            )
        ]
    else:
        outputs = run_all(
            provider_name=provider_name,
            scope_config=scope_config,
            snapshot_dir=args.snapshot_dir,
            config_path=args.config,
            force=args.force,
            top_k=args.top_k,
            max_pool_size=args.max_pool_size,
            min_overlap=args.min_overlap,
            min_candidate_score=args.min_candidate_score,
            min_abs_return_corr=args.min_abs_return_corr,
        )
    for output in outputs:
        print(output)


if __name__ == "__main__":
    main()
