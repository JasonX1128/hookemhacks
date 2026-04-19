from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .common import PipelinePaths
from .schemas import MarketMetadataRecord
from .utils import read_json


def load_metadata_records(paths: PipelinePaths) -> list[MarketMetadataRecord]:
    payload = read_json(paths.metadata_artifact_path)
    return [MarketMetadataRecord.from_mapping(record) for record in payload.get("records", [])]


def load_history_frame(paths: PipelinePaths) -> pd.DataFrame:
    frame = pd.read_csv(
        paths.history_artifact_path,
        usecols=["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"],
        dtype={
            "market_id": "string",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "float64",
            "source": "string",
        },
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values(["market_id", "timestamp"])
    return frame


def load_candidates_records(paths: PipelinePaths) -> list[dict]:
    payload = read_json(paths.candidates_artifact_path)
    return list(payload.get("records", []))


def _load_history_series_from_per_market_cache(paths: PipelinePaths, market_ids: set[str]) -> dict[str, pd.Series]:
    series_by_market: dict[str, pd.Series] = {}
    for market_id in sorted(market_ids):
        market_path = paths.history_cache_dir / f"{market_id}.csv"
        if not market_path.exists():
            continue
        frame = pd.read_csv(
            market_path,
            usecols=["timestamp", "close"],
            dtype={"close": "float64"},
        )
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
        if frame.empty:
            continue
        series_by_market[market_id] = frame.set_index("timestamp")["close"].astype(float)
    return series_by_market


def _load_history_series_from_aggregate(paths: PipelinePaths, market_ids: set[str] | None) -> dict[str, pd.Series]:
    read_kwargs = {
        "usecols": ["market_id", "timestamp", "close"],
        "dtype": {
            "market_id": "string",
            "close": "float64",
        },
    }
    if market_ids is None:
        frame = pd.read_csv(paths.history_artifact_path, **read_kwargs)
    else:
        chunks: list[pd.DataFrame] = []
        for chunk in pd.read_csv(paths.history_artifact_path, chunksize=200_000, **read_kwargs):
            filtered = chunk[chunk["market_id"].isin(sorted(market_ids))]
            if not filtered.empty:
                chunks.append(filtered)
        frame = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=["market_id", "timestamp", "close"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"]).sort_values(["market_id", "timestamp"])
    series_by_market: dict[str, pd.Series] = {}
    for market_id, group in frame.groupby("market_id"):
        series_by_market[str(market_id)] = group.set_index("timestamp")["close"].astype(float).sort_index()
    return series_by_market


def load_history_series_by_market(
    paths: PipelinePaths,
    market_ids: Iterable[str] | None = None,
) -> dict[str, pd.Series]:
    normalized_market_ids = {str(market_id) for market_id in market_ids or [] if market_id}
    if normalized_market_ids:
        cached_series = _load_history_series_from_per_market_cache(paths, normalized_market_ids)
        if len(cached_series) == len(normalized_market_ids):
            return cached_series
        missing_market_ids = normalized_market_ids - set(cached_series)
        aggregate_series = _load_history_series_from_aggregate(paths, missing_market_ids) if paths.history_artifact_path.exists() else {}
        cached_series.update(aggregate_series)
        return cached_series
    return _load_history_series_from_aggregate(paths, None)


def artifact_relative_path(paths: PipelinePaths, path: Path) -> str:
    return str(path.relative_to(paths.base_dir))
