from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .artifact_io import artifact_relative_path, load_candidates_records, load_history_series_by_market
from .common import PAIR_FEATURES_SCHEMA_VERSION, PipelinePaths
from .scope import PipelineScopeConfig, add_scope_arguments, default_scope_config, resolve_scope_from_args
from .utils import safe_corr, update_artifact_manifest


def _lead_lag_summary(primary_returns: pd.Series, related_returns: pd.Series) -> tuple[int, str, float | None]:
    best_lag = 0
    best_direction = "synchronous"
    best_corr: float | None = safe_corr(primary_returns, related_returns)
    max_lag = min(3, len(primary_returns) - 1)
    for lag in range(1, max_lag + 1):
        primary_leads = safe_corr(primary_returns.iloc[:-lag], related_returns.iloc[lag:])
        related_leads = safe_corr(primary_returns.iloc[lag:], related_returns.iloc[:-lag])
        if primary_leads is not None and (best_corr is None or abs(primary_leads) > abs(best_corr)):
            best_corr = primary_leads
            best_lag = lag
            best_direction = "primary_leads"
        if related_leads is not None and (best_corr is None or abs(related_leads) > abs(best_corr)):
            best_corr = related_leads
            best_lag = -lag
            best_direction = "related_leads"
    return best_lag, best_direction, best_corr


def _shock_summary(primary_returns: pd.Series, related_returns: pd.Series) -> tuple[int, float | None, float | None]:
    if len(primary_returns) < 5:
        return 0, None, None
    threshold = primary_returns.abs().quantile(0.8)
    shocks = pd.concat([primary_returns.rename("primary"), related_returns.rename("related")], axis=1)
    shocks = shocks[shocks["primary"].abs() >= threshold]
    if shocks.empty:
        return 0, None, None
    same_direction = (
        np.sign(shocks["primary"]).replace(0.0, np.nan) == np.sign(shocks["related"]).replace(0.0, np.nan)
    ).mean()
    return int(len(shocks)), float(same_direction), float(shocks["related"].mean())


def _beta_and_residuals(aligned: pd.DataFrame) -> tuple[float | None, float | None, float | None, float | None]:
    if len(aligned) < 5 or aligned["primary"].nunique() < 2 or aligned["related"].nunique() < 2:
        return None, None, None, None
    beta, intercept = np.polyfit(aligned["primary"], aligned["related"], 1)
    residuals = aligned["related"] - (intercept + beta * aligned["primary"])
    residual_std = residuals.std(ddof=1)
    latest_residual = float(residuals.iloc[-1])
    latest_residual_zscore = None
    if residual_std and not pd.isna(residual_std) and residual_std > 0:
        latest_residual_zscore = float(latest_residual / residual_std)
    return float(beta), float(intercept), latest_residual, latest_residual_zscore


PAIR_FEATURE_COLUMNS = [
    "market_id",
    "related_market_id",
    "candidate_rank",
    "candidate_score",
    "family_alignment_score",
    "category_overlap_score",
    "semantic_similarity_score",
    "time_horizon_overlap_score",
    "quick_return_correlation",
    "market_primary_family",
    "candidate_primary_family",
    "shared_families",
    "cross_family_link",
    "cluster_ids",
    "shared_terms",
    "overlap_points",
    "overlap_start",
    "overlap_end",
    "price_level_correlation",
    "return_correlation",
    "rolling_corr_mean",
    "rolling_corr_max_abs",
    "lead_lag_best_lag",
    "lead_lag_direction",
    "lead_lag_best_corr",
    "shock_windows",
    "shock_same_direction_ratio",
    "shock_avg_partner_move",
    "spread_volatility",
    "spread_change_volatility",
    "beta",
    "intercept",
    "latest_residual",
    "latest_residual_zscore",
    "comovement_score",
]


def run(*, provider_name: str = "mock", scope_config: PipelineScopeConfig | None = None) -> Path:
    scope_config = scope_config or default_scope_config(provider_name=provider_name)
    paths = PipelinePaths(provider_name=provider_name, scope_slug=scope_config.scope_slug)
    candidates = load_candidates_records(paths)
    history_by_market = load_history_series_by_market(paths)

    rows: list[dict] = []
    for candidate in candidates:
        primary_id = candidate["market_id"]
        related_id = candidate["candidate_market_id"]
        left = history_by_market.get(primary_id)
        right = history_by_market.get(related_id)
        if left is None or right is None:
            continue

        aligned = pd.concat([left.rename("primary"), right.rename("related")], axis=1).dropna()
        overlap_points = int(len(aligned))
        primary_returns = aligned["primary"].diff().dropna()
        related_returns = aligned["related"].diff().dropna()
        returns = pd.concat([primary_returns.rename("primary"), related_returns.rename("related")], axis=1).dropna()

        return_corr = safe_corr(returns["primary"], returns["related"]) if not returns.empty else None
        price_level_corr = safe_corr(aligned["primary"], aligned["related"]) if overlap_points >= 3 else None
        rolling_window = min(7, max(3, len(returns) // 3)) if len(returns) >= 6 else None
        rolling_corr = None
        rolling_abs_max = None
        if rolling_window and rolling_window < len(returns):
            rolling_series = returns["primary"].rolling(rolling_window).corr(returns["related"]).dropna()
            if not rolling_series.empty:
                rolling_corr = float(rolling_series.mean())
                rolling_abs_max = float(rolling_series.abs().max())

        best_lag, lead_lag_direction, best_lag_corr = _lead_lag_summary(returns["primary"], returns["related"]) if len(returns) >= 4 else (0, "insufficient_history", None)
        shock_windows, shock_same_direction_ratio, shock_avg_partner_move = _shock_summary(returns["primary"], returns["related"])
        spread = aligned["primary"] - aligned["related"]
        spread_volatility = float(spread.std(ddof=1)) if overlap_points >= 3 else None
        spread_change_volatility = float(spread.diff().dropna().std(ddof=1)) if overlap_points >= 4 else None
        beta, intercept, latest_residual, latest_residual_zscore = _beta_and_residuals(aligned)

        comovement_score_components = [
            abs(return_corr) if return_corr is not None else 0.0,
            abs(best_lag_corr) if best_lag_corr is not None else 0.0,
            shock_same_direction_ratio if shock_same_direction_ratio is not None else 0.0,
        ]
        comovement_score = round(min(1.0, 0.45 * comovement_score_components[0] + 0.25 * comovement_score_components[1] + 0.3 * comovement_score_components[2]), 4)

        rows.append(
            {
                "market_id": primary_id,
                "related_market_id": related_id,
                "candidate_rank": int(candidate["rank"]),
                "candidate_score": float(candidate["candidate_score"]),
                "family_alignment_score": float(candidate.get("family_alignment_score") or 0.0),
                "category_overlap_score": float(candidate["category_overlap_score"]),
                "semantic_similarity_score": float(candidate["semantic_similarity_score"]),
                "time_horizon_overlap_score": float(candidate["time_horizon_overlap_score"]),
                "quick_return_correlation": candidate["quick_return_correlation"],
                "market_primary_family": candidate.get("market_primary_family"),
                "candidate_primary_family": candidate.get("candidate_primary_family"),
                "shared_families": "|".join(candidate.get("shared_families", [])),
                "cross_family_link": bool(candidate.get("cross_family_link", False)),
                "cluster_ids": "|".join(candidate.get("cluster_ids", [])),
                "shared_terms": "|".join(candidate.get("shared_terms", [])),
                "overlap_points": overlap_points,
                "overlap_start": aligned.index.min().isoformat().replace("+00:00", "Z") if overlap_points else None,
                "overlap_end": aligned.index.max().isoformat().replace("+00:00", "Z") if overlap_points else None,
                "price_level_correlation": round(float(price_level_corr), 4) if price_level_corr is not None else None,
                "return_correlation": round(float(return_corr), 4) if return_corr is not None else None,
                "rolling_corr_mean": round(float(rolling_corr), 4) if rolling_corr is not None else None,
                "rolling_corr_max_abs": round(float(rolling_abs_max), 4) if rolling_abs_max is not None else None,
                "lead_lag_best_lag": best_lag,
                "lead_lag_direction": lead_lag_direction,
                "lead_lag_best_corr": round(float(best_lag_corr), 4) if best_lag_corr is not None else None,
                "shock_windows": shock_windows,
                "shock_same_direction_ratio": round(float(shock_same_direction_ratio), 4) if shock_same_direction_ratio is not None else None,
                "shock_avg_partner_move": round(float(shock_avg_partner_move), 4) if shock_avg_partner_move is not None else None,
                "spread_volatility": round(float(spread_volatility), 6) if spread_volatility is not None else None,
                "spread_change_volatility": round(float(spread_change_volatility), 6) if spread_change_volatility is not None else None,
                "beta": round(float(beta), 6) if beta is not None else None,
                "intercept": round(float(intercept), 6) if intercept is not None else None,
                "latest_residual": round(float(latest_residual), 6) if latest_residual is not None else None,
                "latest_residual_zscore": round(float(latest_residual_zscore), 4) if latest_residual_zscore is not None else None,
                "comovement_score": comovement_score,
            }
        )

    frame = pd.DataFrame(rows, columns=PAIR_FEATURE_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values(["market_id", "candidate_rank", "related_market_id"])
    frame.to_csv(paths.pair_features_artifact_path, index=False)
    update_artifact_manifest(
        manifest_path=paths.artifact_manifest_path,
        artifact_key="pair_features",
        relative_path=artifact_relative_path(paths, paths.pair_features_artifact_path),
        schema_version=PAIR_FEATURES_SCHEMA_VERSION,
        record_count=int(len(frame)),
        extra={"scope_id": scope_config.scope_id},
    )
    return paths.pair_features_artifact_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute light co-movement features for plausible candidate pairs.")
    add_scope_arguments(parser)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    provider_name, scope_config = resolve_scope_from_args(args)
    artifact_path = run(provider_name=provider_name, scope_config=scope_config)
    print(artifact_path)


if __name__ == "__main__":
    main()
