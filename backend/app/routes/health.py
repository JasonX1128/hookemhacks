from __future__ import annotations

from fastapi import APIRouter

from backend.app.storage.db import DATABASE_PATH

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "market-move-explainer-backend",
        "database": str(DATABASE_PATH),
    }
