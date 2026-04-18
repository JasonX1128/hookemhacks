from __future__ import annotations

import argparse
from pathlib import Path

from . import build_candidates, compute_cointegration, compute_comovement, fetch_history, fetch_markets
from .reporting import write_run_summary
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, resolve_scope_from_args


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
