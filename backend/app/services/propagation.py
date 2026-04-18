from __future__ import annotations

from ..schemas.contracts import (
    CatalystCandidate,
    MarketClickContext,
    MoveDirection,
    MoveSummary,
    RelatedMarket,
)
from .utils import clamp_score


class PropagationService:
    """Builds the move summary and mock downstream propagation outputs."""

    def build_move_summary(self, context: MarketClickContext) -> MoveSummary:
        fallback_price = context.clickedPrice if context.clickedPrice is not None else 0.5
        price_before = (
            context.priceBefore if context.priceBefore is not None else max(0.0, fallback_price - 0.08)
        )
        price_after = context.priceAfter if context.priceAfter is not None else fallback_price
        delta = price_after - price_before
        move_direction: MoveDirection = "flat"
        if delta > 0.01:
            move_direction = "up"
        elif delta < -0.01:
            move_direction = "down"

        move_magnitude = round(abs(delta), 2)
        jump_score = clamp_score(0.12 + abs(delta) * 3.4, digits=2)
        return MoveSummary(
            moveMagnitude=move_magnitude,
            moveDirection=move_direction,
            jumpScore=jump_score,
        )

    def propagate_to_related_markets(
        self,
        *,
        move_summary: MoveSummary,
        related_markets: list[RelatedMarket],
    ) -> list[RelatedMarket]:
        propagated_markets: list[RelatedMarket] = []
        for index, market in enumerate(related_markets):
            expected_reaction = clamp_score(
                market.relationStrength * 0.7 + move_summary.jumpScore * 0.3 - index * 0.04,
                digits=2,
            )
            residual_zscore = round(max(0.2, 2.2 - market.relationStrength * 1.4 + index * 0.45), 2)
            status = "normal"
            if residual_zscore >= 2.0:
                status = "divergent"
            elif expected_reaction >= 0.65 and index > 0:
                status = "possibly_lagging"

            direction_phrase = {
                "up": "upside follow-through",
                "down": "downside sympathy",
                "flat": "muted spillover",
            }[move_summary.moveDirection]
            propagated_markets.append(
                market.model_copy(
                    update={
                        "expectedReactionScore": expected_reaction,
                        "residualZscore": residual_zscore,
                        "status": status,
                        "note": (
                            f"Mock propagation suggests {direction_phrase} with "
                            f"{expected_reaction:.2f} expected reaction."
                        ),
                    }
                )
            )
        return propagated_markets

    def compute_confidence(
        self,
        *,
        move_summary: MoveSummary,
        top_catalyst: CatalystCandidate | None,
        related_markets: list[RelatedMarket],
    ) -> float:
        catalyst_score = top_catalyst.totalScore if top_catalyst and top_catalyst.totalScore else 0.45
        average_relation = (
            sum(market.relationStrength for market in related_markets) / len(related_markets)
            if related_markets
            else 0.4
        )
        return clamp_score(
            catalyst_score * 0.5 + move_summary.jumpScore * 0.3 + average_relation * 0.2,
            digits=2,
        )
