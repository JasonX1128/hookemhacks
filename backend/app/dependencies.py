from __future__ import annotations

from functools import lru_cache

from .services.attribution_service import AttributionService


@lru_cache(maxsize=1)
def get_attribution_service() -> AttributionService:
    return AttributionService()
