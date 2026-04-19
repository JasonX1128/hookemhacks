from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")


def _read_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"0", "false", "no", "off"}


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str = "HookEmHacks Backend"
    app_version: str = "0.1.0"
    environment: str = "development"
    host: str = "127.0.0.1"
    port: int = 8000
    mock_mode: bool = True
    cors_origin_regex: str = (
        r"^(https?://(localhost|127\.0\.0\.1)(:\d+)?|chrome-extension://.*)$"
    )
    serper_api_key: str | None = None
    vertex_project_id: str | None = None
    vertex_location: str = "us-central1"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("BACKEND_APP_NAME", "HookEmHacks Backend"),
        app_version=os.getenv("BACKEND_APP_VERSION", "0.1.0"),
        environment=os.getenv("BACKEND_ENV", "development"),
        host=os.getenv("BACKEND_HOST", "127.0.0.1"),
        port=int(os.getenv("BACKEND_PORT", "8000")),
        mock_mode=_read_bool("BACKEND_MOCK_MODE", True),
        serper_api_key=os.getenv("SERPER_API_KEY"),
        vertex_project_id=os.getenv("VERTEX_PROJECT_ID"),
        vertex_location=os.getenv("VERTEX_LOCATION", "us-central1"),
    )
