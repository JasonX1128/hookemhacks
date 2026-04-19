from __future__ import annotations

import sqlite3
from pathlib import Path

DATABASE_PATH = Path(__file__).resolve().parents[2] / "local_cache.sqlite3"

CREATE_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cache_entries (
    namespace TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (namespace, cache_key)
)
"""


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_connection() as connection:
            connection.execute(CREATE_CACHE_TABLE_SQL)
            connection.commit()
    except sqlite3.Error:
        # The app can fall back to the in-memory cache layer when the local SQLite file
        # is unavailable in the current environment.
        return

