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
    data_quality: float


def _parse_iso8601(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _coerce_price(value: object) -> float | None:
    if isinstance(value, dict):
        raw = value.get("close_dollars") or value.get("close") or value.get("mean")
        return _coerce_price(raw)

    try:
        price = float(value)
    except (TypeError, ValueError):
        return None

    if price > 1:
        price /= 100
    return price


def _coerce_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_candle_points(candles: Iterable[dict]) -> list[tuple[float, float | None]]:
    points: list[tuple[float, float | None]] = []
    for candle in candles:
        bid = _coerce_price(candle.get("yes_bid"))
        ask = _coerce_price(candle.get("yes_ask"))
        if bid is not None and ask is not None:
            midpoint = round((bid + ask) / 2, 4)
        else:
            midpoint = _coerce_price(candle.get("price"))
            if midpoint is None:
                midpoint = _coerce_price(candle.get("close")) or _coerce_price(candle.get("yes_price"))

        if midpoint is None:
            continue

        volume = _coerce_float(candle.get("volume_fp") or candle.get("volume"))
        points.append((midpoint, volume))
    return points


class MoveAnalyzer:
    def __init__(self, kalshi_client: KalshiClient | None = None) -> None:
        self.kalshi_client = kalshi_client or KalshiClient()

    def _resolve_series_ticker(self, market_id: str) -> str | None:
        market = self.kalshi_client.fetch_market(market_id)
        series_ticker = None
        event_ticker = None

        if isinstance(market, dict):
            raw_series = market.get("series_ticker")
            raw_event = market.get("event_ticker")
            if isinstance(raw_series, str) and raw_series.strip():
                series_ticker = raw_series
            if isinstance(raw_event, str) and raw_event.strip():
                event_ticker = raw_event

        if series_ticker:
            return series_ticker

        fetch_event = getattr(self.kalshi_client, "fetch_event", None)
        if event_ticker and callable(fetch_event):
            event = fetch_event(event_ticker)
            if isinstance(event, dict):
                raw_series = event.get("series_ticker")
                if isinstance(raw_series, str) and raw_series.strip():
                    return raw_series

        return None

    def _estimate_data_quality(
        self,
        context: MarketClickContext,
        *,
        used_candles: bool,
        used_fallback: bool,
    ) -> float:
        if used_candles:
            return 0.85
        if context.priceBefore is not None and context.priceAfter is not None:
            return 0.55
        if (context.priceBefore is not None or context.priceAfter is not None) and context.clickedPrice is not None:
            return 0.42
        if context.clickedPrice is not None:
            return 0.32 if not used_fallback else 0.24
        return 0.18

    def _calculate_jump_score(
        self,
        before: float,
        after: float,
        candle_points: list[tuple[float, float | None]],
    ) -> float:
        magnitude = abs(after - before)
        jump_score = 0.18 + magnitude * 3.2

        candle_prices = [price for price, _ in candle_points]
        if len(candle_prices) >= 2:
            step_sizes = [abs(current - previous) for previous, current in zip(candle_prices, candle_prices[1:])]
            total_path = sum(step_sizes)
            if total_path > 0:
                jump_score += 0.16 * (max(step_sizes) / total_path)

            candle_volumes = [volume for _, volume in candle_points if volume is not None]
            if len(candle_volumes) >= 2:
                baseline_volume = sum(candle_volumes[:-1]) / max(1, len(candle_volumes) - 1)
                if baseline_volume > 0:
                    volume_ratio = candle_volumes[-1] / baseline_volume
                    jump_score += 0.12 * _clamp((volume_ratio - 1.0) / 3.0)

        return round(_clamp(jump_score), 4)

    def characterize_move(self, context: MarketClickContext) -> MoveAnalysis:
        before = context.priceBefore
        after = context.priceAfter if context.priceAfter is not None else context.clickedPrice
        candle_points: list[tuple[float, float | None]] = []
        used_candles = False
        used_fallback = False

        # Prefer Kalshi candle history when the click context is sparse so we can anchor the move.
        if before is None or after is None:
            candles = self.kalshi_client.fetch_candlesticks(
                context.marketId,
                series_ticker=self._resolve_series_ticker(context.marketId),
                window_start=_parse_iso8601(context.windowStart),
                window_end=_parse_iso8601(context.windowEnd),
            )
            candle_points = _extract_candle_points(candles)
            if candle_points:
                used_candles = True
                before = before if before is not None else candle_points[0][0]
                after = after if after is not None else candle_points[-1][0]

        if before is None and after is None and context.clickedPrice is not None:
            before = max(0.0, context.clickedPrice - 0.04)
            after = min(1.0, context.clickedPrice + 0.04)
            used_fallback = True
        elif before is None and after is not None:
            before = max(0.0, after - 0.05)
            used_fallback = True
        elif after is None and before is not None:
            after = min(1.0, before + 0.05)
            used_fallback = True

        before = before if before is not None else 0.5
        after = after if after is not None else 0.5

        magnitude = round(abs(after - before), 4)
        if magnitude < 0.01:
            direction = "flat"
        elif after > before:
            direction = "up"
        else:
            direction = "down"

        return MoveAnalysis(
            summary=MoveSummary(
                moveMagnitude=magnitude,
                moveDirection=direction,
                jumpScore=self._calculate_jump_score(before, after, candle_points),
            ),
            normalized_before=before,
            normalized_after=after,
            data_quality=self._estimate_data_quality(
                context,
                used_candles=used_candles,
                used_fallback=used_fallback,
            ),
        )
