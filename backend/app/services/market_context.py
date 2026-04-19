from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.app.schemas.contracts import MarketClickContext

from .kalshi_client import KalshiClient


def _normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    normalized = " ".join(value.split()).strip()
    return normalized or None


@dataclass(slots=True)
class MarketContextService:
    kalshi_client: KalshiClient | None = None

    def __post_init__(self) -> None:
        if self.kalshi_client is None:
            self.kalshi_client = KalshiClient()

    def hydrate_context(self, context: MarketClickContext) -> MarketClickContext:
        if not context.marketId:
            return context
        if context.marketSubtitle and context.marketRulesPrimary:
            return context

        market = self.kalshi_client.fetch_market(context.marketId)
        if not isinstance(market, dict):
            return context

        market_id = _normalize_text(
            market.get("ticker") or market.get("market_ticker") or market.get("event_ticker")
        ) or context.marketId
        market_title = _normalize_text(
            market.get("title") or market.get("name") or market.get("question")
        ) or context.marketTitle
        market_subtitle = _normalize_text(market.get("subtitle")) or context.marketSubtitle
        market_rules_primary = _normalize_text(market.get("rules_primary")) or context.marketRulesPrimary
        market_question = (
            _normalize_text(market.get("question"))
            or market_subtitle
            or context.marketQuestion
            or market_rules_primary
            or market_title
        )

        return context.model_copy(
            update={
                "marketId": market_id,
                "marketTitle": market_title,
                "marketQuestion": market_question,
                "marketSubtitle": market_subtitle,
                "marketRulesPrimary": market_rules_primary,
            }
        )
