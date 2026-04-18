from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.app.storage.db import get_connection


class CacheRepository:
    def get_json(self, namespace: str, cache_key: str, *, max_age_seconds: int | None = None) -> Any | None:
        with get_connection() as connection:
            row = connection.execute(
                "SELECT payload, updated_at FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                (namespace, cache_key),
            ).fetchone()

        if row is None:
            return None

        if max_age_seconds is not None:
            updated_at = datetime.fromisoformat(row["updated_at"])
            if datetime.now(UTC) - updated_at > timedelta(seconds=max_age_seconds):
                return None

        return json.loads(row["payload"])

    def set_json(self, namespace: str, cache_key: str, payload: Any) -> None:
        serialized = json.dumps(payload)
        with get_connection() as connection:
            connection.execute(
                """
                INSERT INTO cache_entries (namespace, cache_key, payload, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(namespace, cache_key)
                DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
                """,
                (namespace, cache_key, serialized, datetime.now(UTC).isoformat()),
            )
            connection.commit()
