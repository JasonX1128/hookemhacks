from __future__ import annotations

from fastapi import APIRouter

from ...core.config import get_settings
from ...schemas.contracts import HealthResponse

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        service=settings.app_name,
        mockMode=settings.mock_mode,
        environment=settings.environment,
    )
