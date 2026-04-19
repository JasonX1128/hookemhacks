from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app.main import app

client = TestClient(app)
FIXTURE_DIR = Path(__file__).resolve().parents[1] / "app" / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_health_endpoint_reports_backend_status() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "market-move-explainer-backend"
    assert payload["database"].endswith("backend/local_cache.sqlite3")


def test_attribute_move_returns_contract_shape_and_lagging_signal() -> None:
    response = client.post("/attribute_move", json=load_fixture("mock_market_click_context.json"))

    assert response.status_code == 200
    body = response.json()
    assert body["primaryMarket"]["marketId"] == "KXINFLATION-CPI-MAY2026-ABOVE35"
    assert body["moveSummary"]["moveDirection"] == "up"
    assert body["confidence"] > 0
    assert body["confidence"] < 0.9
    assert body["topCatalyst"]["type"] in {"headline", "scheduled_event", "platform_signal"}
    assert 1 <= len(body["alternativeCatalysts"]) <= 3
    assert len(body["evidence"]) >= 2
    assert body["evidence"][0]["id"] == body["topCatalyst"]["id"]
    assert len(body["relatedMarkets"]) >= 1
    assert any(
        market["status"] in {"possibly_lagging", "divergent"}
        or ("worth checking" in (market.get("note") or "").lower())
        for market in body["relatedMarkets"]
    )
