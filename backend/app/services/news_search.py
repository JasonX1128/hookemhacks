from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from backend.app.schemas.contracts import MarketClickContext

logger = logging.getLogger(__name__)

SERPER_NEWS_URL = "https://google.serper.dev/news"
DEFAULT_TIMEOUT = 5.0
MAX_RESULTS = 10


@dataclass(frozen=True)
class NewsArticle:
    title: str
    url: str
    source: str
    snippet: str | None = None
    date: str | None = None


class NewsSearchService:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key

    def _build_query(self, context: MarketClickContext) -> str:
        title_words = context.marketTitle.split()[:6]
        query = " ".join(title_words)
        return query

    async def search(
        self,
        context: MarketClickContext,
        num_results: int = MAX_RESULTS,
    ) -> list[NewsArticle]:
        if not self.api_key:
            logger.warning("Serper API key not configured, skipping news search")
            return []

        query = self._build_query(context)
        if not query.strip():
            return []

        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "q": query,
            "num": num_results,
        }

        try:
            async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
                response = await client.post(
                    SERPER_NEWS_URL,
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
        except httpx.TimeoutException:
            logger.warning("Serper API request timed out for query: %s", query)
            return []
        except httpx.HTTPStatusError as exc:
            logger.warning("Serper API error %d: %s", exc.response.status_code, exc.response.text[:200])
            return []
        except Exception as exc:
            logger.exception("Unexpected error during Serper search: %s", exc)
            return []

        return self._parse_response(data)

    def _parse_response(self, data: dict) -> list[NewsArticle]:
        articles: list[NewsArticle] = []
        news_items = data.get("news", [])

        for item in news_items:
            if not isinstance(item, dict):
                continue

            title = item.get("title", "")
            link = item.get("link", "")
            if not title or not link:
                continue

            articles.append(
                NewsArticle(
                    title=title,
                    url=link,
                    source=item.get("source", "Unknown"),
                    snippet=item.get("snippet"),
                    date=item.get("date"),
                )
            )

        return articles

    def search_sync(
        self,
        context: MarketClickContext,
        num_results: int = MAX_RESULTS,
    ) -> list[NewsArticle]:
        if not self.api_key:
            logger.warning("Serper API key not configured, skipping news search")
            return []

        query = self._build_query(context)
        if not query.strip():
            return []

        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "q": query,
            "num": num_results,
        }

        try:
            with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
                response = client.post(
                    SERPER_NEWS_URL,
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
        except httpx.TimeoutException:
            logger.warning("Serper API request timed out for query: %s", query)
            return []
        except httpx.HTTPStatusError as exc:
            logger.warning("Serper API error %d: %s", exc.response.status_code, exc.response.text[:200])
            return []
        except Exception as exc:
            logger.exception("Unexpected error during Serper search: %s", exc)
            return []

        return self._parse_response(data)
