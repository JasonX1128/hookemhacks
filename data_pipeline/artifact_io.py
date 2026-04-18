from __future__ import annotations

from pathlib import Path

import pandas as pd

from .common import PipelinePaths
from .schemas import MarketMetadataRecord
from .utils import read_json


def load_metadata_records(paths: PipelinePaths) -> list[MarketMetadataRecord]:
    payload = read_json(paths.metadata_artifact_path)
    return [MarketMetadataRecord.from_mapping(record) for record in payload.get("records", [])]


def load_history_frame(paths: PipelinePaths) -> pd.DataFrame:
    frame = pd.read_csv(paths.history_artifact_path)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values(["market_id", "timestamp"])
    return frame


def load_candidates_records(paths: PipelinePaths) -> list[dict]:
    payload = read_json(paths.candidates_artifact_path)
    return list(payload.get("records", []))


def load_history_series_by_market(paths: PipelinePaths) -> dict[str, pd.Series]:
    frame = load_history_frame(paths)
    series_by_market: dict[str, pd.Series] = {}
    for market_id, group in frame.groupby("market_id"):
        series_by_market[market_id] = group.set_index("timestamp")["close"].astype(float).sort_index()
    return series_by_market


def artifact_relative_path(paths: PipelinePaths, path: Path) -> str:
    return str(path.relative_to(paths.base_dir))

