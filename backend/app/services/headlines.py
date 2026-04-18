from __future__ import annotations

import json
from pathlib import Path

from backend.app.models.contracts import MarketClickContext, RetrievedCatalystCandidate
from backend.app.storage.cache_repo import CacheRepository
from backend.app.services.utils import clamp_score, parse_timestamp, token_overlap


class HeadlinesService:
    def __init__(self, cache_repo: CacheRepository | None = None) -> None:
        self.cache_repo = cache_repo or CacheRepository()
        self.fixture_path = Path(__file__).resolve().parents[3] / "data_pipeline" / "fixtures" / "macro_headlines_demo.json"
        self.lookback_minutes = 180
        self.lookahead_minutes = 20

    def _load_fixture(self) -> list[dict]:
        cached = self.cache_repo.get_json("fixture", "macro_headlines_demo", max_age_seconds=60)
        if cached is None:
            with self.fixture_path.open("r", encoding="utf-8") as fixture_file:
                cached = json.load(fixture_file)
            self.cache_repo.set_json("fixture", "macro_headlines_demo", cached)
        return cached

    def retrieve(self, context: MarketClickContext) -> list[RetrievedCatalystCandidate]:
        market_text = f"{context.marketTitle} {context.marketQuestion}".lower()
        clicked = parse_timestamp(context.clickedTimestamp)
        candidates: list[RetrievedCatalystCandidate] = []

        for headline in self._load_fixture():
            if not isinstance(headline, dict):
                continue

            timestamp = str(headline.get("timestamp", context.clickedTimestamp))
            minutes_delta = (parse_timestamp(timestamp) - clicked).total_seconds() / 60
            if minutes_delta < -self.lookback_minutes or minutes_delta > self.lookahead_minutes:
                continue

            keywords = [str(value) for value in headline.get("keywords", [])]
            candidate_text = " ".join(
                part
                for part in [
                    str(headline.get("title", "")),
                    str(headline.get("snippet", "")),
                    " ".join(keywords),
                ]
                if part
            ).lower()
            keyword_match = any(keyword.lower() in market_text for keyword in keywords)
            relevance = token_overlap(market_text, candidate_text)
            if not keyword_match and relevance < 0.08:
                continue

            candidates.append(
                RetrievedCatalystCandidate(
                    id=str(headline.get("id")),
                    type="headline",
                    title=str(headline.get("title")),
                    timestamp=timestamp,
                    source=str(headline.get("source", "Stub headline fixture")),
                    snippet=str(headline.get("snippet")) if headline.get("snippet") else None,
                    url=str(headline.get("url")) if headline.get("url") else None,
                    importance=clamp_score(float(headline.get("importance", 0.55))),
                    keywords=keywords,
                )
            )

        # TODO: Add an adapter for a live macro/news feed if we choose a stable provider later.
        return candidates

    def get_candidates(self, context: MarketClickContext) -> list[dict[str, object]]:
        return [candidate.model_dump() for candidate in self.retrieve(context)]
