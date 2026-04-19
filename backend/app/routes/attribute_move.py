from __future__ import annotations

from fastapi import APIRouter, Depends

from backend.app.dependencies import get_attribution_service
from backend.app.models.contracts import AttributionResponse, MarketClickContext
from backend.app.services.attribution_service import AttributionService

router = APIRouter(tags=["attribution"])


@router.post("/attribute_move", response_model=AttributionResponse, response_model_exclude_none=True)
def attribute_move(
    context: MarketClickContext,
    attribution_service: AttributionService = Depends(get_attribution_service),
) -> AttributionResponse:
    return attribution_service.attribute_move(context)
