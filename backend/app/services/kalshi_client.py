from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from backend.app.storage.cache_repo import CacheRepository

KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiClient:
    def __init__(self, cache_repo: CacheRepository | None = None) -> None:
        self.cache_repo = cache_repo or CacheRepository()

    def _get_json(self, path: str, *, params: dict[str, Any] | None = None, cache_ttl_seconds: int = 300) -> Any | None:
        cache_key = f"{path}?{urlencode(sorted((params or {}).items()))}"
        cached = self.cache_repo.get_json("kalshi_http", cache_key, max_age_seconds=cache_ttl_seconds)
        if cached is not None:
            return cached

        url = f"{KALSHI_API_BASE}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"

        request = Request(url, headers={"User-Agent": "kalshify/0.1"})
        try:
            with urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            return None

        self.cache_repo.set_json("kalshi_http", cache_key, payload)
        return payload

    def fetch_market(self, market_id: str) -> dict[str, Any] | None:
        payload = self._get_json(f"/markets/{market_id}", cache_ttl_seconds=600)
        if isinstance(payload, dict):
            if "market" in payload and isinstance(payload["market"], dict):
                return payload["market"]
            return payload
        return None

    def fetch_event(self, event_ticker: str) -> dict[str, Any] | None:
        payload = self._get_json(f"/events/{event_ticker}", cache_ttl_seconds=600)
        if isinstance(payload, dict):
            if "event" in payload and isinstance(payload["event"], dict):
                return payload["event"]
            return payload
        return None

    def fetch_candlesticks(
        self,
        market_id: str,
        *,
        series_ticker: str | None,
        window_start: datetime,
        window_end: datetime,
        period_interval: int = 1,
    ) -> list[dict[str, Any]]:
        if not series_ticker:
            return []

        payload = self._get_json(
            f"/series/{series_ticker}/markets/{market_id}/candlesticks",
            params={
                "start_ts": int(window_start.timestamp()),
                "end_ts": int(window_end.timestamp()),
                "period_interval": period_interval,
                "include_latest_before_start": "true",
            },
            cache_ttl_seconds=300,
        )

        if isinstance(payload, dict) and isinstance(payload.get("candlesticks"), list):
            return [item for item in payload["candlesticks"] if isinstance(item, dict)]
        return []

