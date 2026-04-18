from __future__ import annotations

import argparse

from .main import run_all
from .scope import add_scope_arguments, default_scope_config_path, resolve_scope_from_args


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full preprocessing pipeline in local/mock mode.")
    add_scope_arguments(parser)
    parser.add_argument("--force", action="store_true", help="Refresh cached metadata/history before rebuilding downstream artifacts.")
    parser.add_argument(
        "--use-default-config",
        action="store_true",
        help=f"Convenience flag for using the sample config at {default_scope_config_path()}",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.use_default_config and args.config is None:
        args.config = default_scope_config_path()
    provider_name, scope_config = resolve_scope_from_args(args)
    outputs = run_all(
        provider_name=provider_name,
        scope_config=scope_config,
        config_path=args.config,
        force=args.force,
        top_k=args.top_k,
        max_pool_size=args.max_pool_size,
    )
    for output in outputs:
        print(output)


if __name__ == "__main__":
    main()
