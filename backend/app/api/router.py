from __future__ import annotations

from fastapi import APIRouter

from .routes.attribute_move import router as attribute_move_router
from .routes.health import router as health_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(attribute_move_router)
