from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from backend.app.schemas.contracts import MarketClickContext

logger = logging.getLogger(__name__)

SERPER_NEWS_URL = "https://google.serper.dev/news"
DEFAULT_TIMEOUT = 8.0
MAX_RESULTS = 15


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
        title_lower = context.marketTitle.lower()
        question_lower = context.marketQuestion.lower()
        combined = f"{title_lower} {question_lower}"
        market_id = (context.marketId or "").lower()

        sport_leagues = {
            "nba": "NBA basketball",
            "nfl": "NFL football",
            "mlb": "MLB baseball",
            "nhl": "NHL hockey",
            "wnba": "WNBA basketball",
            "mls": "MLS soccer",
            "ufc": "UFC MMA",
            "pga": "PGA golf",
            "nascar": "NASCAR racing",
            "f1": "Formula 1 racing",
            "atp": "ATP tennis",
            "wta": "WTA tennis",
        }

        detected_league = None
        for league, full_name in sport_leagues.items():
            if league in combined or league in market_id:
                detected_league = full_name
                break

        sports_keywords = ["playoffs", "series", "game", "match", "championship", "finals", " vs ", " vs. "]
        is_sports = detected_league or any(kw in combined for kw in sports_keywords)

        # "vs" indicates sports but don't assume which sport
        if " vs " in title_lower or " vs. " in title_lower:
            is_sports = True

        # Words to exclude from queries
        stop_words = {
            "will", "the", "win", "vs", "beat", "defeat", "series", "game", "in", "to",
            "and", "or", "a", "an", "nba", "nfl", "mlb", "nhl", "track", "what", "kalshi",
            "market", "prediction", "probability", "chance", "odds", "yes", "no", "over", "under"
        }

        if is_sports:
            team_names = []
            for word in context.marketTitle.split():
                clean = word.strip("?.,!'\"").lower()
                if clean not in stop_words and len(clean) > 1:
                    team_names.append(word.strip("?.,!'\""))

            league_prefix = detected_league + " " if detected_league else ""
            query = league_prefix + " ".join(team_names[:3]) + " latest news"
        else:
            # For non-sports, just use the title cleaned up
            query = context.marketTitle
            # Remove common meta-phrases
            for phrase in ["Track what", "Kalshi's", "prediction market"]:
                query = query.replace(phrase, "").strip()

        return query.strip()

    def _filter_irrelevant(self, articles: list[NewsArticle]) -> list[NewsArticle]:
        blocked_domains = ["kalshi.com", "polymarket.com", "predictit.org", "metaculus.com"]
        blocked_terms = ["betting odds", "spread", "moneyline", "over/under", "sportsbook", "wager", "parlay", "picks and predictions"]

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
        num_results: int = MAX_RESULTS,
    ) -> list[NewsArticle]:
        if not self.api_key:
            logger.warning("Serper API key not configured, skipping news search")
            return []

        query = self._build_query(context)
        print(f"[DEBUG] News search query: {query}")
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
                print(f"[DEBUG] Serper returned {len(data.get('news', []))} raw articles")
        except httpx.TimeoutException:
            logger.warning("Serper API request timed out for query: %s", query)
            return []
        except httpx.HTTPStatusError as exc:
            logger.warning("Serper API error %d: %s", exc.response.status_code, exc.response.text[:200])
            return []
        except Exception as exc:
            logger.exception("Unexpected error during Serper search: %s", exc)
            return []

        filtered = self._filter_irrelevant(self._parse_response(data))
        print(f"[DEBUG] After filtering: {len(filtered)} articles")
        return filtered
