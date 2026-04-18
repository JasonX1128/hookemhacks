from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint

from .artifact_io import artifact_relative_path, load_history_series_by_market
from .common import COINTEGRATION_SCHEMA_VERSION, PipelinePaths
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, resolve_scope_from_args
from .utils import update_artifact_manifest


def _estimate_half_life(spread: pd.Series) -> float | None:
    if len(spread) < 10 or spread.nunique() < 2:
        return None
    lagged = spread.shift(1).dropna()
    delta = spread.diff().dropna()
    regression_frame = pd.concat([delta.rename("delta"), lagged.rename("lagged")], axis=1).dropna()
    if regression_frame.empty or regression_frame["lagged"].nunique() < 2:
        return None
    model = sm.OLS(regression_frame["delta"], sm.add_constant(regression_frame["lagged"])).fit()
    speed = float(model.params.iloc[1])
    if speed >= 0:
        return None
    half_life = -math.log(2) / speed
    if not np.isfinite(half_life):
        return None
    return float(half_life)


def _quality_flag(*, p_value: float | None, half_life: float | None, beta: float | None) -> str:
    if p_value is None:
        return "skipped"
    if beta is None or abs(beta) < 0.05 or abs(beta) > 10:
        return "reject"
    if p_value < 0.05 and half_life is not None and 1 <= half_life <= 45:
        return "strong"
    if p_value < 0.1 and half_life is not None and 1 <= half_life <= 90:
        return "tentative"
    return "weak"


COINTEGRATION_COLUMNS = [
    "market_id",
    "related_market_id",
    "enough_history",
    "eligible_for_test",
    "overlapping_samples",
    "hedge_ratio",
    "intercept",
    "test_statistic",
    "p_value",
    "critical_value_5pct",
    "spread_stationary_flag",
    "half_life",
    "quality_flag",
    "skip_reason",
]


def run(
    *,
    provider_name: str = "mock",
    scope_config: PipelineScopeConfig | None = None,
    min_overlap: int = 30,
    min_candidate_score: float = 0.55,
    min_abs_return_corr: float = 0.25,
) -> Path:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    pair_features = pd.read_csv(paths.pair_features_artifact_path)
    history_by_market = load_history_series_by_market(paths)

    rows: list[dict] = []
    for feature_row in pair_features.to_dict(orient="records"):
        market_id = str(feature_row["market_id"])
        related_market_id = str(feature_row["related_market_id"])
        overlap_points = int(feature_row.get("overlap_points") or 0)
        candidate_score = float(feature_row.get("candidate_score") or 0.0)
        return_corr = feature_row.get("return_correlation")
        enough_history = overlap_points >= min_overlap
        eligible = enough_history and candidate_score >= min_candidate_score and abs(float(return_corr or 0.0)) >= min_abs_return_corr

        left = history_by_market.get(market_id)
        right = history_by_market.get(related_market_id)
        aligned = pd.concat([left.rename("left"), right.rename("right")], axis=1).dropna() if left is not None and right is not None else pd.DataFrame()

        result = {
            "market_id": market_id,
            "related_market_id": related_market_id,
            "enough_history": enough_history,
            "eligible_for_test": eligible,
            "overlapping_samples": int(len(aligned)),
            "hedge_ratio": None,
            "intercept": None,
            "test_statistic": None,
            "p_value": None,
            "critical_value_5pct": None,
            "spread_stationary_flag": False,
            "half_life": None,
            "quality_flag": "skipped",
            "skip_reason": None,
        }

        if not eligible:
            skip_reason = []
            if not enough_history:
                skip_reason.append("insufficient_history")
            if candidate_score < min_candidate_score:
                skip_reason.append("low_candidate_score")
            if abs(float(return_corr or 0.0)) < min_abs_return_corr:
                skip_reason.append("weak_return_correlation")
            result["skip_reason"] = "|".join(skip_reason) if skip_reason else "ineligible"
            rows.append(result)
            continue

        if len(aligned) < min_overlap or aligned["left"].nunique() < 2 or aligned["right"].nunique() < 2:
            result["skip_reason"] = "insufficient_aligned_levels"
            rows.append(result)
            continue

        try:
            model = sm.OLS(aligned["right"], sm.add_constant(aligned["left"])).fit()
            intercept = float(model.params.iloc[0])
            hedge_ratio = float(model.params.iloc[1])
            # Event-market price levels are bounded and regime-sensitive, so this is only a cautious
            # optional signal for plausible pairs rather than a robust truth test.
            test_statistic, p_value, critical_values = coint(aligned["right"], aligned["left"])
            spread = aligned["right"] - (intercept + hedge_ratio * aligned["left"])
            half_life = _estimate_half_life(spread)
            quality_flag = _quality_flag(p_value=float(p_value), half_life=half_life, beta=hedge_ratio)

            result.update(
                {
                    "hedge_ratio": round(hedge_ratio, 6),
                    "intercept": round(intercept, 6),
                    "test_statistic": round(float(test_statistic), 6),
                    "p_value": round(float(p_value), 6),
                    "critical_value_5pct": round(float(critical_values[1]), 6),
                    "spread_stationary_flag": bool(float(p_value) < 0.05),
                    "half_life": round(float(half_life), 4) if half_life is not None else None,
                    "quality_flag": quality_flag,
                    "skip_reason": None,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive path for unstable statistical routines
            result["quality_flag"] = "error"
            result["skip_reason"] = type(exc).__name__

        rows.append(result)

    frame = pd.DataFrame(rows, columns=COINTEGRATION_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values(["market_id", "related_market_id"])
    frame.to_csv(paths.cointegration_artifact_path, index=False)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="cointegration_metrics",
        relative_path=artifact_relative_path(paths, paths.cointegration_artifact_path),
        schema_version=COINTEGRATION_SCHEMA_VERSION,
        record_count=int(len(frame)),
        extra={
            "scope_id": scope_config.scope_id,
            "notes": [
                "Cointegration is intentionally conservative and only evaluated on already-filtered candidate pairs.",
                "Bounded event-market probabilities can break classical assumptions, so weak or skipped results are expected.",
            ]
        },
    )
    return paths.cointegration_artifact_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Optionally compute conservative cointegration metrics for candidate pairs.")
    add_scope_arguments(parser)
    parser.add_argument("--min-overlap", type=int, default=30, help="Minimum aligned samples required before testing.")
    parser.add_argument(
        "--min-candidate-score",
        type=float,
        default=0.55,
        help="Minimum candidate score required before testing cointegration.",
    )
    parser.add_argument(
        "--min-abs-return-corr",
        type=float,
        default=0.25,
        help="Minimum absolute return correlation required before testing cointegration.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    provider_name, scope_config = resolve_scope_from_args(args)
    artifact_path = run(
        provider_name=provider_name,
        scope_config=scope_config,
        min_overlap=args.min_overlap,
        min_candidate_score=args.min_candidate_score,
        min_abs_return_corr=args.min_abs_return_corr,
    )
    print(artifact_path)


if __name__ == "__main__":
    main()
