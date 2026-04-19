from __future__ import annotations

from fastapi import APIRouter

from backend.app.core.config import get_settings
from backend.app.services.pipeline_runner import pipeline_runner

router = APIRouter(tags=["pipeline"])


@router.post("/pipeline/startup_refresh")
def trigger_startup_refresh() -> dict[str, object]:
    result = pipeline_runner.start_startup_refresh(get_settings())
    return result.to_dict()


@router.post("/pipeline/stop_refresh")
def stop_startup_refresh() -> dict[str, object]:
    result = pipeline_runner.stop_startup_refresh(get_settings())
    return result.to_dict()


@router.get("/pipeline/startup_status")
def get_startup_refresh_status() -> dict[str, object]:
    result = pipeline_runner.current_startup_status(get_settings())
    return result.to_dict()
