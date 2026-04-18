from __future__ import annotations

import json
from pathlib import Path

from backend.app.models.contracts import MarketClickContext, RetrievedCatalystCandidate
from backend.app.storage.cache_repo import CacheRepository
from backend.app.services.utils import clamp_score, parse_timestamp, token_overlap


class ScheduledEventsService:
    def __init__(self, cache_repo: CacheRepository | None = None) -> None:
        self.cache_repo = cache_repo or CacheRepository()
        self.fixture_path = Path(__file__).resolve().parents[3] / "data_pipeline" / "fixtures" / "macro_calendar.json"
        self.lookback_minutes = 240
        self.lookahead_minutes = 60

    def _load_fixture(self) -> list[dict]:
        cached = self.cache_repo.get_json("fixture", "macro_calendar", max_age_seconds=60)
        if cached is None:
            with self.fixture_path.open("r", encoding="utf-8") as fixture_file:
                cached = json.load(fixture_file)
            self.cache_repo.set_json("fixture", "macro_calendar", cached)
        return cached

    def retrieve(self, context: MarketClickContext) -> list[RetrievedCatalystCandidate]:
        market_text = f"{context.marketTitle} {context.marketQuestion}".lower()
        clicked = parse_timestamp(context.clickedTimestamp)
        candidates: list[RetrievedCatalystCandidate] = []

        for event in self._load_fixture():
            if not isinstance(event, dict):
                continue

            timestamp = str(event.get("timestamp", context.clickedTimestamp))
            minutes_delta = (parse_timestamp(timestamp) - clicked).total_seconds() / 60
            if minutes_delta < -self.lookback_minutes or minutes_delta > self.lookahead_minutes:
                continue

            keywords = [str(value) for value in event.get("keywords", [])]
            candidate_text = " ".join(
                part
                for part in [
                    str(event.get("title", "")),
                    str(event.get("snippet", "")),
                    " ".join(keywords),
                ]
                if part
            ).lower()
            keyword_match = any(keyword.lower() in market_text for keyword in keywords)
            relevance = token_overlap(market_text, candidate_text)
            if not keyword_match and relevance < 0.06:
                continue

            candidates.append(
                RetrievedCatalystCandidate(
                    id=str(event.get("id")),
                    type="scheduled_event",
                    title=str(event.get("title")),
                    timestamp=timestamp,
                    source=str(event.get("source", "Stub scheduled-event fixture")),
                    snippet=str(event.get("snippet")) if event.get("snippet") else None,
                    url=str(event.get("url")) if event.get("url") else None,
                    importance=clamp_score(float(event.get("importance", 0.6))),
                    keywords=keywords,
                )
            )

        # TODO: Add optional public calendar enrichment when a stable no-key source is selected.
        return candidates

    def get_candidates(self, context: MarketClickContext) -> list[dict[str, object]]:
        return [candidate.model_dump() for candidate in self.retrieve(context)]
