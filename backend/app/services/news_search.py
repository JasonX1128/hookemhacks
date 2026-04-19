from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from backend.app.schemas.contracts import MarketClickContext
from backend.app.services.utils import parse_timestamp

logger = logging.getLogger(__name__)

SERPER_NEWS_URL = "https://google.serper.dev/news"
DEFAULT_TIMEOUT = 8.0
MAX_RESULTS = 15
SEARCH_WINDOW_PADDING = timedelta(hours=12)
RECENT_CLICK_THRESHOLD = timedelta(hours=30)


@dataclass(frozen=True)
class NewsArticle:
    title: str
    url: str
    source: str
    snippet: str | None = None
    date: str | None = None
    relevanceScore: float | None = None
    alignmentScore: float | None = None


class NewsSearchService:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key

    def _build_query(self, context: MarketClickContext) -> str:
        preferred_parts = [
            context.marketTitle,
            context.marketSubtitle or "",
            context.marketQuestion or "",
        ]
        query = " ".join(part.strip() for part in preferred_parts if part and part.strip())
        query = query.replace("Track what", "").replace("prediction market", "").replace("Kalshi's", "")
        return " ".join(query.split()).strip()

    def build_time_filter(
        self,
        context: MarketClickContext,
        *,
        now: datetime | None = None,
    ) -> str | None:
        now_utc = now.astimezone(UTC) if now is not None else datetime.now(UTC)
        clicked_at = parse_timestamp(context.clickedTimestamp)
        if abs(now_utc - clicked_at) <= RECENT_CLICK_THRESHOLD:
            return "qdr:d"

        window_start = parse_timestamp(context.windowStart)
        window_end = parse_timestamp(context.windowEnd)
        search_start = min(window_start, clicked_at) - SEARCH_WINDOW_PADDING
        search_end = max(window_end, clicked_at) + SEARCH_WINDOW_PADDING
        if search_end < search_start:
            search_end = search_start

        return "cdr:1,cd_min:{},cd_max:{}".format(
            search_start.strftime("%m/%d/%Y"),
            search_end.strftime("%m/%d/%Y"),
        )

    def _filter_irrelevant(self, articles: list[NewsArticle]) -> list[NewsArticle]:
        blocked_domains = ["kalshi.com", "polymarket.com", "predictit.org", "metaculus.com"]
        blocked_terms = [
            "betting odds",
            "spread",
            "moneyline",
            "over/under",
            "sportsbook",
            "wager",
            "parlay",
            "picks and predictions",
        ]

        filtered = []
        for article in articles:
            url_lower = article.url.lower()
            title_lower = article.title.lower()
            snippet_lower = (article.snippet or "").lower()

            if any(domain in url_lower for domain in blocked_domains):
                continue
            if any(term in title_lower or term in snippet_lower for term in blocked_terms):
                continue
            filtered.append(article)

        return filtered

    async def search(
        self,
        context: MarketClickContext,
        *,
        search_query: str | None = None,
        tbs: str | None = None,
        num_results: int = MAX_RESULTS,
    ) -> list[NewsArticle]:
        if not self.api_key:
            logger.warning("Serper API key not configured, skipping news search")
            return []

        query = (search_query or self._build_query(context)).strip()
        if not query:
            return []

        payload = {
            "q": query,
            "num": num_results,
        }
        resolved_tbs = tbs or self.build_time_filter(context)
        if resolved_tbs:
            payload["tbs"] = resolved_tbs

        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
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

        return self._filter_irrelevant(self._parse_response(data))

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
        *,
        search_query: str | None = None,
        tbs: str | None = None,
        num_results: int = MAX_RESULTS,
    ) -> list[NewsArticle]:
        if not self.api_key:
            logger.warning("Serper API key not configured, skipping news search")
            return []

        query = (search_query or self._build_query(context)).strip()
        if not query:
            return []

        payload = {
            "q": query,
            "num": num_results,
        }
        resolved_tbs = tbs or self.build_time_filter(context)
        if resolved_tbs:
            payload["tbs"] = resolved_tbs

        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }

        logger.debug("Serper news search query=%s tbs=%s", query, resolved_tbs)
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

        return self._filter_irrelevant(self._parse_response(data))
