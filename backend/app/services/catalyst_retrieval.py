from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from ..schemas.contracts import MarketClickContext, MoveSummary, RetrievedCatalystCandidate
from .headlines import HeadlinesService
from .scheduled_events import ScheduledEventsService
from .utils import clamp_score


class CandidateCatalystRetriever(Protocol):
    def retrieve(self, context: MarketClickContext) -> list[RetrievedCatalystCandidate]:
        """Retrieve typed candidate catalysts for a clicked market."""


@dataclass(slots=True)
class CatalystRetrievalService:
    retrievers: Sequence[CandidateCatalystRetriever] | None = None
    include_platform_signal: bool = True

    def __post_init__(self) -> None:
        if self.retrievers is None:
            self.retrievers = (
                ScheduledEventsService(),
                HeadlinesService(),
            )

    def retrieve(
        self,
        context: MarketClickContext,
        move_summary: MoveSummary | None = None,
    ) -> list[RetrievedCatalystCandidate]:
        candidates: list[RetrievedCatalystCandidate] = []
        seen_ids: set[str] = set()

        for retriever in self.retrievers or ():
            for candidate in retriever.retrieve(context):
                if candidate.id in seen_ids:
                    continue
                candidates.append(candidate)
                seen_ids.add(candidate.id)

        if self.include_platform_signal:
            platform_signal = self._platform_signal_candidate(context, move_summary)
            if platform_signal.id not in seen_ids:
                candidates.append(platform_signal)

        return candidates

    def _platform_signal_candidate(
        self,
        context: MarketClickContext,
        move_summary: MoveSummary | None,
    ) -> RetrievedCatalystCandidate:
        jump_score = move_summary.jumpScore if move_summary is not None else 0.45
        move_direction = move_summary.moveDirection if move_summary is not None else "flat"
        move_phrase = {
            "up": "upside move",
            "down": "downside move",
            "flat": "local move",
        }[move_direction]
        return RetrievedCatalystCandidate(
            id=f"platform-signal-{context.marketId}",
            type="platform_signal",
            title=f"Local price action flagged a sharp {move_phrase} worth cross-checking",
            timestamp=context.clickedTimestamp,
            source="Local move analyzer",
            snippet=(
                "Platform signal only: abrupt local price action can help prioritize nearby catalysts, "
                "but it does not establish causality by itself."
            ),
            importance=clamp_score(0.28 + jump_score * 0.35),
            keywords=["price-action", "platform-signal"],
            directionalHint=move_direction,
        )
