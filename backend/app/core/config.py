from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _read_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"0", "false", "no", "off"}


def _default_pipeline_startup_python() -> str:
    env_override = os.getenv("BACKEND_PIPELINE_STARTUP_PYTHON")
    if env_override:
        return env_override

    repo_root = Path(__file__).resolve().parents[3]
    venv_python = repo_root / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)

    return "python3"


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str = "HookEmHacks Backend"
    app_version: str = "0.1.0"
    environment: str = "development"
    host: str = "127.0.0.1"
    port: int = 8000
    mock_mode: bool = True
    pipeline_startup_enabled: bool = True
    pipeline_startup_config: str = "data_pipeline/configs/kalshi_live_all_pages.json"
    pipeline_startup_python: str = "python3"
    pipeline_startup_cooldown_seconds: int = 600
    cors_origin_regex: str = (
        r"^(https?://(localhost|127\.0\.0\.1)(:\d+)?|chrome-extension://.*)$"
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("BACKEND_APP_NAME", "HookEmHacks Backend"),
        app_version=os.getenv("BACKEND_APP_VERSION", "0.1.0"),
        environment=os.getenv("BACKEND_ENV", "development"),
        host=os.getenv("BACKEND_HOST", "127.0.0.1"),
        port=int(os.getenv("BACKEND_PORT", "8000")),
        mock_mode=_read_bool("BACKEND_MOCK_MODE", True),
        pipeline_startup_enabled=_read_bool("BACKEND_PIPELINE_STARTUP_ENABLED", True),
        pipeline_startup_config=os.getenv(
            "BACKEND_PIPELINE_STARTUP_CONFIG",
            "data_pipeline/configs/kalshi_live_all_pages.json",
        ),
        pipeline_startup_python=_default_pipeline_startup_python(),
        pipeline_startup_cooldown_seconds=int(os.getenv("BACKEND_PIPELINE_STARTUP_COOLDOWN_SECONDS", "600")),
    )
