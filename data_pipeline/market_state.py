from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from .schemas import MarketMetadataRecord
from .utils import parse_timestamp


OPEN_MARKET_STATUSES = {"active", "open", "paused", "initialized", "unopened"}


def normalize_market_status(status: str | None) -> str:
    return str(status or "").strip().lower()


def market_is_concluded(
    *,
    status: str | None = None,
    close_time: str | None = None,
    resolution_time: str | None = None,
) -> bool:
    normalized_status = normalize_market_status(status)
    if normalized_status and normalized_status not in OPEN_MARKET_STATUSES:
        return True
    market_end = parse_timestamp(resolution_time) or parse_timestamp(close_time)
    if market_end is None:
        return False
    return bool(market_end <= pd.Timestamp.now(tz="UTC"))


def market_record_is_concluded(record: MarketMetadataRecord) -> bool:
    return market_is_concluded(
        status=record.status,
        close_time=record.close_time,
        resolution_time=record.resolution_time,
    )


def mapping_market_is_concluded(record: Mapping[str, Any] | None) -> bool:
    if not record:
        return False
    return market_is_concluded(
        status=_mapping_str(record, "status", "marketStatus"),
        close_time=_mapping_str(record, "close_time", "closeTime"),
        resolution_time=_mapping_str(
            record,
            "resolution_time",
            "resolutionTime",
            "settlement_ts",
            "settlementTs",
        ),
    )


def merge_market_records(
    existing_records: Iterable[MarketMetadataRecord],
    incoming_records: Iterable[MarketMetadataRecord],
) -> list[MarketMetadataRecord]:
    merged_by_id: dict[str, MarketMetadataRecord] = {
        record.market_id: record
        for record in existing_records
        if record.market_id
    }
    for record in incoming_records:
        if record.market_id:
            merged_by_id[record.market_id] = record
    return sorted(merged_by_id.values(), key=lambda record: record.market_id)


def prune_concluded_market_records(records: Iterable[MarketMetadataRecord]) -> list[MarketMetadataRecord]:
    return [record for record in records if not market_record_is_concluded(record)]


def _mapping_str(record: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = record.get(key)
        if value not in {"", None}:
            return str(value)
    return None
