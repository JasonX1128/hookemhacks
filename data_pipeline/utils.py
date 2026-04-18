from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


STOPWORDS = {
    "a",
    "above",
    "after",
    "an",
    "and",
    "are",
    "as",
    "at",
    "august",
    "before",
    "be",
    "below",
    "by",
    "december",
    "dollar",
    "dollars",
    "during",
    "end",
    "february",
    "for",
    "from",
    "high",
    "higher",
    "in",
    "is",
    "it",
    "january",
    "july",
    "june",
    "least",
    "lower",
    "march",
    "may",
    "new",
    "november",
    "of",
    "october",
    "on",
    "or",
    "q1",
    "q2",
    "q3",
    "q4",
    "record",
    "records",
    "september",
    "that",
    "the",
    "this",
    "trade",
    "trades",
    "trading",
    "to",
    "will",
    "with",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False)


def update_artifact_manifest(
    *,
    manifest_path: Path,
    artifact_key: str,
    relative_path: str,
    schema_version: str,
    record_count: int | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    manifest = read_json(manifest_path) if manifest_path.exists() else {"generated_at": utc_now_iso(), "artifacts": {}}
    entry: dict[str, Any] = {
        "path": relative_path,
        "schema_version": schema_version,
        "updated_at": utc_now_iso(),
    }
    if record_count is not None:
        entry["record_count"] = record_count
    if extra:
        entry.update(extra)
    manifest["artifacts"][artifact_key] = entry
    manifest["generated_at"] = utc_now_iso()
    write_json(manifest_path, manifest)


def parse_timestamp(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp


def format_timestamp(value: pd.Timestamp | str | None) -> str | None:
    if value is None:
        return None
    timestamp = parse_timestamp(str(value))
    if timestamp is None:
        return None
    return timestamp.isoformat().replace("+00:00", "Z")


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]+", " ", value.lower())).strip()


def tokenize_text(*parts: str | None) -> set[str]:
    tokens: list[str] = []
    for part in parts:
        normalized = normalize_text(part)
        if not normalized:
            continue
        for token in normalized.split(" "):
            if len(token) <= 2 or token in STOPWORDS:
                continue
            if token.isdigit() or re.fullmatch(r"20\d{2}", token):
                continue
            tokens.append(token)
    return set(tokens)


def token_jaccard(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def semantic_similarity(left: str, right: str) -> float:
    normalized_left = normalize_text(left)
    normalized_right = normalize_text(right)
    if not normalized_left or not normalized_right:
        return 0.0
    token_score = token_jaccard(normalized_left.split(" "), normalized_right.split(" "))
    sequence_score = SequenceMatcher(None, normalized_left, normalized_right).ratio()
    return round((0.65 * token_score) + (0.35 * sequence_score), 4)


def safe_corr(left: pd.Series, right: pd.Series) -> float | None:
    paired = pd.concat([left, right], axis=1).dropna()
    if len(paired) < 3:
        return None
    left_values = paired.iloc[:, 0]
    right_values = paired.iloc[:, 1]
    if left_values.nunique() < 2 or right_values.nunique() < 2:
        return None
    corr = left_values.corr(right_values)
    if pd.isna(corr):
        return None
    return float(corr)


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def logistic(value: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-value))


def horizon_bucket(open_time: str | None, resolution_time: str | None) -> str:
    opened_at = parse_timestamp(open_time)
    resolved_at = parse_timestamp(resolution_time)
    if opened_at is None or resolved_at is None:
        return "unknown"
    days = max(1, int((resolved_at - opened_at).total_seconds() // 86_400))
    if days <= 30:
        return "0_30d"
    if days <= 90:
        return "31_90d"
    if days <= 180:
        return "91_180d"
    return "181d_plus"


def time_overlap_score(
    left_open: str | None,
    left_close: str | None,
    left_resolution: str | None,
    right_open: str | None,
    right_close: str | None,
    right_resolution: str | None,
) -> float:
    left_start = parse_timestamp(left_open)
    right_start = parse_timestamp(right_open)
    left_end = parse_timestamp(left_close) or parse_timestamp(left_resolution)
    right_end = parse_timestamp(right_close) or parse_timestamp(right_resolution)
    resolution_left = parse_timestamp(left_resolution)
    resolution_right = parse_timestamp(right_resolution)

    overlap_score = 0.0
    if left_start is not None and right_start is not None and left_end is not None and right_end is not None:
        intersection_start = max(left_start, right_start)
        intersection_end = min(left_end, right_end)
        union_start = min(left_start, right_start)
        union_end = max(left_end, right_end)
        intersection = max(0.0, (intersection_end - intersection_start).total_seconds())
        union = max(1.0, (union_end - union_start).total_seconds())
        overlap_score = intersection / union

    resolution_score = 0.0
    if resolution_left is not None and resolution_right is not None:
        days_apart = abs((resolution_left - resolution_right).days)
        resolution_score = math.exp(-days_apart / 45.0)

    return round((0.55 * overlap_score) + (0.45 * resolution_score), 4)


def build_json_envelope(
    *,
    artifact_name: str,
    provider_name: str,
    schema_version: str,
    record_key: str,
    records: list[dict[str, Any]],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "artifact": artifact_name,
        "provider": provider_name,
        "schema_version": schema_version,
        "generated_at": utc_now_iso(),
        record_key: records,
    }
    if extra:
        payload.update(extra)
    return payload


def normalize_history_frame(history_frame: pd.DataFrame, market_id: str) -> pd.DataFrame:
    frame = history_frame.copy()
    if "timestamp" not in frame.columns:
        raise ValueError(f"history for {market_id} is missing a timestamp column")
    if "close" not in frame.columns:
        if "price" in frame.columns:
            frame["close"] = frame["price"]
        else:
            raise ValueError(f"history for {market_id} is missing a close or price column")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
    frame = frame.drop_duplicates(subset=["timestamp"], keep="last")

    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        if column not in frame.columns:
            if column == "volume":
                frame[column] = 0.0
            else:
                frame[column] = frame["close"]
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["close"])
    frame = frame[(frame["close"] >= 0.0) & (frame["close"] <= 1.0)]
    for column in ["open", "high", "low", "close"]:
        frame[column] = frame[column].clip(lower=0.0, upper=1.0)

    frame["high"] = frame[["open", "high", "close"]].max(axis=1)
    frame["low"] = frame[["open", "low", "close"]].min(axis=1)
    frame["volume"] = frame["volume"].fillna(0.0).clip(lower=0.0)
    frame["market_id"] = market_id
    frame["source"] = frame.get("source", "unknown")
    frame["timestamp"] = frame["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return frame[["market_id", "timestamp", "open", "high", "low", "close", "volume", "source"]]
