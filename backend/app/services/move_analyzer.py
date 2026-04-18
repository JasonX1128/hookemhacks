from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable

from backend.app.models.contracts import MarketClickContext, MoveSummary
from backend.app.services.kalshi_client import KalshiClient


@dataclass(slots=True)
class MoveAnalysis:
    summary: MoveSummary
    normalized_before: float
    normalized_after: float


def _parse_iso8601(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)


def _extract_price_from_candles(candles: Iterable[dict]) -> tuple[float | None, float | None]:
    prices: list[float] = []
    for candle in candles:
        price = candle.get("price")
        if not isinstance(price, dict):
            continue
        raw = price.get("close_dollars") or price.get("close") or price.get("mean")
        try:
            prices.append(float(raw))
        except (TypeError, ValueError):
            continue

    if not prices:
        return None, None

    return prices[0], prices[-1]


class MoveAnalyzer:
    def __init__(self, kalshi_client: KalshiClient | None = None) -> None:
        self.kalshi_client = kalshi_client or KalshiClient()

    def characterize_move(self, context: MarketClickContext) -> MoveAnalysis:
        before = context.priceBefore
        after = context.priceAfter if context.priceAfter is not None else context.clickedPrice

        # TODO: Prefer richer Kalshi chart history when we can reliably derive the underlying market ticker and series.
        if before is None or after is None:
            market = self.kalshi_client.fetch_market(context.marketId)
            series_ticker = None
            if isinstance(market, dict):
                series_ticker = market.get("series_ticker")

            candles = self.kalshi_client.fetch_candlesticks(
                context.marketId,
                series_ticker=series_ticker if isinstance(series_ticker, str) else None,
                window_start=_parse_iso8601(context.windowStart),
                window_end=_parse_iso8601(context.windowEnd),
            )
            candle_before, candle_after = _extract_price_from_candles(candles)
            before = before if before is not None else candle_before
            after = after if after is not None else candle_after

        if before is None and after is None and context.clickedPrice is not None:
            before = max(0.0, context.clickedPrice - 0.04)
            after = min(1.0, context.clickedPrice + 0.04)
        elif before is None and after is not None:
            before = max(0.0, after - 0.05)
        elif after is None and before is not None:
            after = min(1.0, before + 0.05)

        before = before if before is not None else 0.5
        after = after if after is not None else 0.5

        magnitude = round(abs(after - before), 4)
        if magnitude < 0.01:
            direction = "flat"
        elif after > before:
            direction = "up"
        else:
            direction = "down"

        jump_score = round(min(1.0, magnitude / 0.2), 4)
        return MoveAnalysis(
            summary=MoveSummary(
                moveMagnitude=magnitude,
                moveDirection=direction,
                jumpScore=jump_score,
            ),
            normalized_before=before,
            normalized_after=after,
        )

