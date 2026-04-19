from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.app.storage.db import get_connection

_MEMORY_CACHE: dict[tuple[str, str], tuple[str, str]] = {}


class CacheRepository:
    def get_json(self, namespace: str, cache_key: str, *, max_age_seconds: int | None = None) -> Any | None:
        try:
            with get_connection() as connection:
                row = connection.execute(
                    "SELECT payload, updated_at FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                    (namespace, cache_key),
                ).fetchone()
        except sqlite3.Error:
            cached = _MEMORY_CACHE.get((namespace, cache_key))
            if cached is None:
                return None
            payload, updated_at_raw = cached
            updated_at = updated_at_raw
            if max_age_seconds is not None:
                cached_at = datetime.fromisoformat(updated_at)
                if datetime.now(UTC) - cached_at > timedelta(seconds=max_age_seconds):
                    return None
            return json.loads(payload)

        if row is None:
            return None

        if max_age_seconds is not None:
            updated_at = datetime.fromisoformat(row["updated_at"])
            if datetime.now(UTC) - updated_at > timedelta(seconds=max_age_seconds):
                return None

        return json.loads(row["payload"])

    def set_json(self, namespace: str, cache_key: str, payload: Any) -> None:
        serialized = json.dumps(payload)
        updated_at = datetime.now(UTC).isoformat()
        try:
            with get_connection() as connection:
                connection.execute(
                    """
                    INSERT INTO cache_entries (namespace, cache_key, payload, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(namespace, cache_key)
                    DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
                    """,
                    (namespace, cache_key, serialized, updated_at),
                )
                connection.commit()
        except sqlite3.Error:
            _MEMORY_CACHE[(namespace, cache_key)] = (serialized, updated_at)
