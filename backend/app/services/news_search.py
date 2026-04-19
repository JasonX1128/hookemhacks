from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import httpx

from backend.app.schemas.contracts import MarketClickContext
from backend.app.services.utils import parse_timestamp

logger = logging.getLogger(__name__)

SERPER_NEWS_URL = "https://google.serper.dev/news"
DEFAULT_TIMEOUT = 8.0
MAX_RESULTS = 15
SEARCH_WINDOW_PADDING = timedelta(hours=12)
RECENT_CLICK_THRESHOLD = timedelta(hours=30)

SOURCE_CREDIBILITY_TIERS: dict[str, float] = {
    "reuters.com": 0.20,
    "apnews.com": 0.20,
    "bbc.com": 0.18,
    "bbc.co.uk": 0.18,
    "nytimes.com": 0.15,
    "washingtonpost.com": 0.15,
    "wsj.com": 0.15,
    "bloomberg.com": 0.15,
    "ft.com": 0.15,
    "economist.com": 0.15,
    "theguardian.com": 0.12,
    "cnn.com": 0.10,
    "espn.com": 0.12,
    "sports.yahoo.com": 0.10,
    "cbssports.com": 0.10,
    "nbcsports.com": 0.10,
    "skysports.com": 0.12,
    "theathletic.com": 0.12,
    "cnbc.com": 0.12,
    "marketwatch.com": 0.10,
    "forbes.com": 0.08,
    "businessinsider.com": 0.08,
    "politico.com": 0.12,
    "thehill.com": 0.10,
    "axios.com": 0.10,
}

LOW_QUALITY_DOMAINS = {
    "medium.com",
    "substack.com",
    "blogspot.com",
    "wordpress.com",
    "tumblr.com",
}


@dataclass(frozen=True)
class NewsArticle:
    title: str
    url: str
    source: str
    snippet: str | None = None
    date: str | None = None
    relevanceScore: float | None = None
    alignmentScore: float | None = None
    credibilityScore: float | None = None
    temporalScore: float | None = None


def get_source_credibility(url: str) -> float:
    try:
        domain = urlparse(url).netloc.lower()
        domain = re.sub(r"^www\.", "", domain)
    except Exception:
        return 0.0

    if domain in SOURCE_CREDIBILITY_TIERS:
        return SOURCE_CREDIBILITY_TIERS[domain]

    for tier_domain, score in SOURCE_CREDIBILITY_TIERS.items():
        if domain.endswith("." + tier_domain) or tier_domain.endswith("." + domain):
            return score

    if domain in LOW_QUALITY_DOMAINS:
        return -0.05

    return 0.0


def compute_temporal_score(article_date: str | None, reference_time: datetime) -> float:
    if not article_date:
        return 0.3

    try:
        parsed = _parse_relative_date(article_date, reference_time)
        if parsed is None:
            return 0.3

        hours_diff = abs((reference_time - parsed).total_seconds()) / 3600
        if hours_diff <= 2:
            return 1.0
        elif hours_diff <= 6:
            return 0.85
        elif hours_diff <= 12:
            return 0.7
        elif hours_diff <= 24:
            return 0.5
        elif hours_diff <= 48:
            return 0.3
        else:
            return max(0.1, 0.3 - (hours_diff - 48) / 240)
    except Exception:
        return 0.3


def _parse_relative_date(date_str: str, reference: datetime) -> datetime | None:
    date_lower = date_str.lower().strip()

    relative_patterns = [
        (r"(\d+)\s*min(?:ute)?s?\s*ago", lambda m: reference - timedelta(minutes=int(m.group(1)))),
        (r"(\d+)\s*hours?\s*ago", lambda m: reference - timedelta(hours=int(m.group(1)))),
        (r"(\d+)\s*days?\s*ago", lambda m: reference - timedelta(days=int(m.group(1)))),
        (r"yesterday", lambda m: reference - timedelta(days=1)),
        (r"today", lambda m: reference),
    ]

    for pattern, handler in relative_patterns:
        match = re.search(pattern, date_lower)
        if match:
            return handler(match)

    return None


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

    def _parse_response(self, data: dict, reference_time: datetime | None = None) -> list[NewsArticle]:
        articles: list[NewsArticle] = []
        news_items = data.get("news", [])
        ref_time = reference_time or datetime.now(UTC)

        for item in news_items:
            if not isinstance(item, dict):
                continue

            title = item.get("title", "")
            link = item.get("link", "")
            if not title or not link:
                continue

            article_date = item.get("date")
            articles.append(
                NewsArticle(
                    title=title,
                    url=link,
                    source=item.get("source", "Unknown"),
                    snippet=item.get("snippet"),
                    date=article_date,
                    credibilityScore=get_source_credibility(link),
                    temporalScore=compute_temporal_score(article_date, ref_time),
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

        reference_time = parse_timestamp(context.clickedTimestamp)

        import time
        logger.debug("[News] Searching: %s (tbs=%s)", query[:80], resolved_tbs)
        search_start = time.perf_counter()
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
            logger.warning("[News] Serper API timed out after %.2fs for: %s", time.perf_counter() - search_start, query[:60])
            return []
        except httpx.HTTPStatusError as exc:
            logger.warning("[News] Serper API error %d: %s", exc.response.status_code, exc.response.text[:200])
            return []
        except Exception as exc:
            logger.exception("[News] Unexpected error during search: %s", exc)
            return []

        raw_articles = self._parse_response(data, reference_time=reference_time)
        filtered_articles = self._filter_irrelevant(raw_articles)
        logger.debug(
            "[News] Found %d articles (%d after filtering) in %.2fs",
            len(raw_articles),
            len(filtered_articles),
            time.perf_counter() - search_start,
        )
        return filtered_articles

    def search_multi_query(
        self,
        context: MarketClickContext,
        queries: list[str],
        *,
        num_results_per_query: int = 10,
    ) -> list[NewsArticle]:
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not self.api_key or not queries:
            return []

        logger.debug("[News] Multi-query search with %d queries", len(queries))
        start_time = time.perf_counter()

        all_articles: list[NewsArticle] = []
        seen_urls: set[str] = set()

        def fetch_query(query: str) -> list[NewsArticle]:
            return self.search_sync(
                context,
                search_query=query,
                num_results=num_results_per_query,
            )

        with ThreadPoolExecutor(max_workers=min(3, len(queries))) as executor:
            futures = {executor.submit(fetch_query, q): q for q in queries[:3]}
            for future in as_completed(futures):
                query = futures[future]
                try:
                    articles = future.result()
                    for article in articles:
                        if article.url not in seen_urls:
                            seen_urls.add(article.url)
                            all_articles.append(article)
                    logger.debug("[News] Query '%s' returned %d articles", query[:40], len(articles))
                except Exception as exc:
                    logger.warning("[News] Query '%s' failed: %s", query[:40], exc)

        logger.debug(
            "[News] Multi-query found %d unique articles in %.2fs",
            len(all_articles),
            time.perf_counter() - start_time,
        )
        return all_articles
