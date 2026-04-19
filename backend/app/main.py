from __future__ import annotations

import logging
import os
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.core.config import get_settings
from backend.app.routes.attribute_move import router as attribute_router
from backend.app.routes.attribute_move import service as attribution_service
from backend.app.routes.health import router as health_router
from backend.app.routes.pipeline import router as pipeline_router
from backend.app.storage.db import init_db


def _configure_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def create_app() -> FastAPI:
    _configure_logging()
    settings = get_settings()
    init_db()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        docs_url="/docs",
        redoc_url="/redoc",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[],
        allow_origin_regex=settings.cors_origin_regex,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health_router)
    app.include_router(attribute_router)
    app.include_router(pipeline_router)

    @app.on_event("startup")
    def prewarm_related_markets_cache() -> None:
        attribution_service.related_markets.prewarm()

    return app


app = create_app()
