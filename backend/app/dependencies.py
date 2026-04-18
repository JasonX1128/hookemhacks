from __future__ import annotations

from functools import lru_cache

from .services.attribution import AttributionService
from .services.catalyst_retrieval import CatalystRetrievalService
from .services.catalyst_scoring import CatalystScoringService
from .services.propagation import PropagationService
from .services.related_markets import RelatedMarketsService


@lru_cache(maxsize=1)
def get_attribution_service() -> AttributionService:
    return AttributionService(
        catalyst_retrieval=CatalystRetrievalService(),
        catalyst_scoring=CatalystScoringService(),
        related_markets=RelatedMarketsService(),
        propagation=PropagationService(),
    )
