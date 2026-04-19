from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from backend.app.schemas.contracts import MarketClickContext, MoveSummary
from backend.app.services.kalshi_client import KalshiClient
from backend.app.services.utils import clamp_score, parse_timestamp
from backend.app.storage.cache_repo import CacheRepository

logger = logging.getLogger(__name__)

LOOKBACK_MINUTES = 60
LOOKAHEAD_MINUTES = 10


@dataclass(frozen=True)
class MarketSnapshot:
    price_at_click: float | None
    price_before: float | None
    price_after: float | None
    volume_24h: int | None
    open_interest: int | None
    last_trade_price: float | None
    candlesticks: list[dict]


@dataclass(frozen=True)
class RealMoveSummary:
    price_before: float
    price_after: float
    move_magnitude: float
    move_direction: str
    confidence: float
    data_source: str


class MarketDataService:
    def __init__(self, cache_repo: CacheRepository | None = None) -> None:
        self.kalshi = KalshiClient(cache_repo or CacheRepository())

    def fetch_market_snapshot(self, context: MarketClickContext) -> MarketSnapshot | None:
        clicked_at = parse_timestamp(context.clickedTimestamp)
        window_start = clicked_at - timedelta(minutes=LOOKBACK_MINUTES)
        window_end = clicked_at + timedelta(minutes=LOOKAHEAD_MINUTES)

        logger.debug(
            "[MarketData] Fetching data for %s around %s",
            context.marketId,
            clicked_at.isoformat(),
        )

        market = self.kalshi.fetch_market(context.marketId)
        if not market:
            logger.debug("[MarketData] Could not fetch market details for %s", context.marketId)
            return None

        series_ticker = market.get("series_ticker") or market.get("ticker", "").split("-")[0]

        candlesticks = self.kalshi.fetch_candlesticks(
            market_id=context.marketId,
            series_ticker=series_ticker,
            window_start=window_start,
            window_end=window_end,
            period_interval=1,
        )

        logger.debug(
            "[MarketData] Fetched %d candlesticks for %s",
            len(candlesticks),
            context.marketId,
        )

        price_at_click = self._find_price_at_time(candlesticks, clicked_at)
        price_before = self._find_price_at_time(candlesticks, clicked_at - timedelta(minutes=30))
        price_after = self._find_price_at_time(candlesticks, clicked_at + timedelta(minutes=5))

        return MarketSnapshot(
            price_at_click=price_at_click,
            price_before=price_before,
            price_after=price_after or price_at_click,
            volume_24h=market.get("volume_24h"),
            open_interest=market.get("open_interest"),
            last_trade_price=self._parse_price(market.get("last_price")),
            candlesticks=candlesticks,
        )

    def compute_real_move(self, context: MarketClickContext) -> RealMoveSummary | None:
        snapshot = self.fetch_market_snapshot(context)

        if snapshot and snapshot.price_before is not None and snapshot.price_after is not None:
            return self._compute_from_candlesticks(snapshot)

        if snapshot and snapshot.last_trade_price is not None:
            return self._compute_from_last_price(context, snapshot)

        return self._compute_from_context(context)

    def _compute_from_candlesticks(self, snapshot: MarketSnapshot) -> RealMoveSummary:
        price_before = snapshot.price_before
        price_after = snapshot.price_after or snapshot.price_at_click or price_before

        delta = price_after - price_before
        magnitude = abs(delta)

        if delta > 0.01:
            direction = "up"
        elif delta < -0.01:
            direction = "down"
        else:
            direction = "flat"

        logger.debug(
            "[MarketData] Computed from candlesticks: %.2f -> %.2f (%s %.1f%%)",
            price_before,
            price_after,
            direction,
            magnitude * 100,
        )

        return RealMoveSummary(
            price_before=price_before,
            price_after=price_after,
            move_magnitude=magnitude,
            move_direction=direction,
            confidence=0.9,
            data_source="kalshi_candlesticks",
        )

    def _compute_from_last_price(self, context: MarketClickContext, snapshot: MarketSnapshot) -> RealMoveSummary:
        current_price = snapshot.last_trade_price

        fallback_before = context.priceBefore if context.priceBefore is not None else max(0.0, current_price - 0.05)

        delta = current_price - fallback_before
        magnitude = abs(delta)

        if delta > 0.01:
            direction = "up"
        elif delta < -0.01:
            direction = "down"
        else:
            direction = "flat"

        logger.debug(
            "[MarketData] Computed from last price: ~%.2f -> %.2f (%s %.1f%%)",
            fallback_before,
            current_price,
            direction,
            magnitude * 100,
        )

        return RealMoveSummary(
            price_before=fallback_before,
            price_after=current_price,
            move_magnitude=magnitude,
            move_direction=direction,
            confidence=0.5,
            data_source="kalshi_last_price",
        )

    def _compute_from_context(self, context: MarketClickContext) -> RealMoveSummary | None:
        if context.priceBefore is None and context.priceAfter is None and context.clickedPrice is None:
            logger.debug("[MarketData] No price data available for %s", context.marketId)
            return None

        price_after = context.priceAfter or context.clickedPrice or 0.5
        price_before = context.priceBefore or max(0.0, price_after - 0.05)

        delta = price_after - price_before
        magnitude = abs(delta)

        if delta > 0.01:
            direction = "up"
        elif delta < -0.01:
            direction = "down"
        else:
            direction = "flat"

        logger.debug(
            "[MarketData] Computed from context: %.2f -> %.2f (%s %.1f%%)",
            price_before,
            price_after,
            direction,
            magnitude * 100,
        )

        return RealMoveSummary(
            price_before=price_before,
            price_after=price_after,
            move_magnitude=magnitude,
            move_direction=direction,
            confidence=0.3,
            data_source="extension_context",
        )

    def _find_price_at_time(self, candlesticks: list[dict], target_time: datetime) -> float | None:
        if not candlesticks:
            return None

        target_ts = target_time.timestamp()
        closest = None
        closest_diff = float("inf")

        for candle in candlesticks:
            candle_ts = candle.get("end_period_ts") or candle.get("start_period_ts")
            if candle_ts is None:
                continue

            diff = abs(candle_ts - target_ts)
            if diff < closest_diff:
                closest_diff = diff
                closest = candle

        if closest is None:
            return None

        close_price = closest.get("close") or closest.get("price") or closest.get("yes_price")
        return self._parse_price(close_price)

    def _parse_price(self, value) -> float | None:
        if value is None:
            return None
        try:
            price = float(value)
            if price > 1:
                price = price / 100
            return price
        except (ValueError, TypeError):
            return None

    def to_move_summary(self, real_move: RealMoveSummary) -> MoveSummary:
        return MoveSummary(
            moveMagnitude=round(real_move.move_magnitude, 4),
            moveDirection=real_move.move_direction,
            jumpScore=clamp_score(0.18 + real_move.move_magnitude * 3.2, digits=2),
        )
