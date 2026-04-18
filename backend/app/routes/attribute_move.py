from __future__ import annotations

from fastapi import APIRouter

from backend.app.models.contracts import AttributionResponse, MarketClickContext
from backend.app.services.attribution_service import AttributionService

router = APIRouter(tags=["attribution"])
service = AttributionService()


@router.post("/attribute_move", response_model=AttributionResponse)
def attribute_move(context: MarketClickContext) -> AttributionResponse:
    return service.attribute_move(context)
