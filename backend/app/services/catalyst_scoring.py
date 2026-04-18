from __future__ import annotations

import math

from ..models.scoring import CatalystScoreBreakdown
from ..schemas.contracts import CatalystCandidate, MarketClickContext, MoveSummary, RetrievedCatalystCandidate
from .utils import clamp_score, parse_timestamp, token_overlap, tokenize_text


class CatalystScoringService:
    """Scores catalyst candidates with conservative ranking heuristics."""

    HAWKISH_TERMS = {
        "hawkish",
        "higher",
        "hot",
        "inflation",
        "prices",
        "rate",
        "rates",
        "rebound",
        "sticky",
        "strong",
        "yields",
    }
    DOVISH_TERMS = {
        "cooling",
        "cut",
        "cuts",
        "dovish",
        "easing",
        "lower",
        "soft",
        "weaker",
    }
    POSITIVE_TERMS = {
        "above",
        "beat",
        "gain",
        "higher",
        "increase",
        "rise",
        "rally",
        "strong",
        "surge",
        "up",
    }
    NEGATIVE_TERMS = {
        "below",
        "decline",
        "decrease",
        "down",
        "drop",
        "fall",
        "lower",
        "miss",
        "soft",
        "weaker",
    }
    TIME_SCALES = {
        "headline": 75.0,
        "platform_signal": 30.0,
        "scheduled_event": 120.0,
    }

    def score(
        self,
        *,
        context: MarketClickContext,
        move_summary: MoveSummary,
        candidates: list[RetrievedCatalystCandidate],
    ) -> list[CatalystCandidate]:
        scored_candidates: list[CatalystCandidate] = []

        for candidate in candidates:
            time_proximity = self._time_score(context.clickedTimestamp, candidate)
            semantic_relevance = self._semantic_score(context, candidate)
            event_importance = clamp_score(candidate.importance)
            source_agreement = self._source_agreement_score(candidate, candidates)
            move_alignment = self._move_alignment_score(context, move_summary, candidate)

            breakdown = CatalystScoreBreakdown(
                time_proximity=time_proximity,
                semantic_relevance=semantic_relevance,
                event_importance=event_importance,
                source_agreement=source_agreement,
                move_alignment=move_alignment,
            )
            scored_candidates.append(
                CatalystCandidate(
                    id=candidate.id,
                    type=candidate.type,
                    title=candidate.title,
                    timestamp=candidate.timestamp,
                    source=candidate.source,
                    snippet=candidate.snippet,
                    url=candidate.url,
                    semanticScore=semantic_relevance,
                    timeScore=time_proximity,
                    importanceScore=event_importance,
                    totalScore=breakdown.total,
                )
            )

        return sorted(scored_candidates, key=lambda candidate: candidate.totalScore or 0.0, reverse=True)

    def select_evidence(
        self,
        *,
        top_catalyst: CatalystCandidate | None,
        ranked_candidates: list[CatalystCandidate],
    ) -> list[CatalystCandidate]:
        if top_catalyst is None or not ranked_candidates:
            return []

        minimum_score = max(0.42, (top_catalyst.totalScore or 0.0) - 0.18)
        evidence: list[CatalystCandidate] = []
        seen_sources: set[str] = set()
        seen_types: set[str] = set()

        for candidate in ranked_candidates:
            if (candidate.totalScore or 0.0) < minimum_score and len(evidence) >= 2:
                continue

            duplicate_shape = candidate.source in seen_sources and candidate.type in seen_types
            if duplicate_shape and len(evidence) >= 3:
                continue

            evidence.append(candidate)
            seen_sources.add(candidate.source)
            seen_types.add(candidate.type)
            if len(evidence) >= 4:
                break

        if top_catalyst.id not in {candidate.id for candidate in evidence}:
            evidence.insert(0, top_catalyst)

        return evidence[:4]

    def compute_confidence(
        self,
        *,
        move_summary: MoveSummary,
        top_catalyst: CatalystCandidate | None,
        alternative_catalysts: list[CatalystCandidate],
        evidence: list[CatalystCandidate],
    ) -> float:
        if top_catalyst is None:
            return 0.12

        top_score = top_catalyst.totalScore or 0.0
        evidence_scores = [candidate.totalScore or 0.0 for candidate in evidence]
        supporting_scores = evidence_scores[1:] if len(evidence_scores) > 1 else []
        support_strength = sum(supporting_scores) / len(supporting_scores) if supporting_scores else 0.0
        diversity = clamp_score(
            0.5 * min(1.0, len({candidate.source for candidate in evidence}) / 3)
            + 0.5 * min(1.0, len({candidate.type for candidate in evidence}) / 3)
        )
        runner_up = alternative_catalysts[0].totalScore if alternative_catalysts else max(0.0, top_score - 0.1)
        separation = clamp_score(0.5 + max(0.0, top_score - (runner_up or 0.0)))

        confidence = clamp_score(
            0.45 * top_score
            + 0.2 * support_strength
            + 0.15 * move_summary.jumpScore
            + 0.1 * diversity
            + 0.1 * separation
        )

        cap = 0.86
        if len(evidence) < 2:
            cap = 0.62
        elif len(evidence) < 3:
            cap = 0.74
        if top_catalyst.type == "platform_signal":
            cap = min(cap, 0.58)

        return round(min(cap, confidence), 4)

    def _candidate_text(self, candidate: RetrievedCatalystCandidate) -> str:
        return " ".join(
            part
            for part in [
                candidate.title,
                candidate.snippet or "",
                " ".join(candidate.keywords),
            ]
            if part
        )

    def _agreement_tokens(self, candidate: RetrievedCatalystCandidate) -> set[str]:
        keyword_tokens = tokenize_text(" ".join(candidate.keywords))
        if keyword_tokens:
            return keyword_tokens
        return tokenize_text(self._candidate_text(candidate))

    def _semantic_score(
        self,
        context: MarketClickContext,
        candidate: RetrievedCatalystCandidate,
    ) -> float:
        context_text = f"{context.marketTitle} {context.marketQuestion}"
        market_tokens = tokenize_text(context_text)
        candidate_text = self._candidate_text(candidate)
        candidate_tokens = tokenize_text(candidate_text)

        if not market_tokens or not candidate_tokens:
            return 0.35

        overlap_ratio = len(market_tokens & candidate_tokens) / max(1, min(len(market_tokens), len(candidate_tokens)))
        loose_overlap = token_overlap(context_text, candidate_text)
        return clamp_score(0.2 + 0.45 * overlap_ratio + 0.35 * loose_overlap)

    def _time_score(
        self,
        clicked_timestamp: str,
        candidate: RetrievedCatalystCandidate,
    ) -> float:
        clicked = parse_timestamp(clicked_timestamp)
        candidate_time = parse_timestamp(candidate.timestamp)
        minutes_apart = abs((clicked - candidate_time).total_seconds()) / 60
        scale = self.TIME_SCALES.get(candidate.type, 90.0)
        return clamp_score(math.exp(-((minutes_apart / scale) ** 1.1)))

    def _source_agreement_score(
        self,
        candidate: RetrievedCatalystCandidate,
        candidates: list[RetrievedCatalystCandidate],
    ) -> float:
        candidate_tokens = self._agreement_tokens(candidate)
        if not candidate_tokens:
            return 0.3 if candidate.type == "platform_signal" else 0.4

        corroborations = 0
        corroborating_sources: set[str] = set()
        corroborating_types: set[str] = set()

        for peer in candidates:
            if peer.id == candidate.id:
                continue

            shared_tokens = candidate_tokens & self._agreement_tokens(peer)
            if len(shared_tokens) >= 2 or (shared_tokens and peer.type != candidate.type):
                corroborations += 1
                corroborating_sources.add(peer.source)
                corroborating_types.add(peer.type)

        score = (
            0.34
            + min(0.32, corroborations * 0.16)
            + min(0.16, len(corroborating_types) * 0.08)
            + min(0.1, len(corroborating_sources) * 0.05)
        )
        if candidate.type == "platform_signal":
            score -= 0.12

        return clamp_score(score)

    def _move_alignment_score(
        self,
        context: MarketClickContext,
        move_summary: MoveSummary,
        candidate: RetrievedCatalystCandidate,
    ) -> float:
        if move_summary.moveDirection == "flat":
            return 0.5

        if candidate.directionalHint is not None:
            if candidate.directionalHint == move_summary.moveDirection:
                return 0.82
            if candidate.directionalHint == "flat":
                return 0.5
            return 0.28

        market_text = f"{context.marketTitle} {context.marketQuestion}".lower()
        candidate_tokens = tokenize_text(self._candidate_text(candidate))
        if not candidate_tokens:
            return 0.45

        upward_terms, downward_terms = self._market_direction_terms(market_text)
        expected_terms = upward_terms if move_summary.moveDirection == "up" else downward_terms
        opposite_terms = downward_terms if move_summary.moveDirection == "up" else upward_terms

        matching_signals = len(candidate_tokens & expected_terms)
        conflicting_signals = len(candidate_tokens & opposite_terms)

        if matching_signals and not conflicting_signals:
            return clamp_score(0.62 + min(0.18, matching_signals * 0.09))
        if conflicting_signals and not matching_signals:
            return clamp_score(0.38 - min(0.16, conflicting_signals * 0.08))
        if matching_signals and conflicting_signals:
            return 0.5

        backstop = token_overlap(market_text, self._candidate_text(candidate).lower())
        return clamp_score(0.44 + backstop * 0.18)

    def _market_direction_terms(self, market_text: str) -> tuple[set[str], set[str]]:
        if "cut" in market_text:
            return (
                self.DOVISH_TERMS | {"cut", "cuts"},
                self.HAWKISH_TERMS,
            )
        if any(term in market_text for term in {"below", "lower", "under", "less than"}):
            return (
                self.NEGATIVE_TERMS | self.DOVISH_TERMS,
                self.POSITIVE_TERMS | self.HAWKISH_TERMS,
            )
        if any(term in market_text for term in {"inflation", "cpi", "pce", "yield", "rates"}):
            return (
                self.HAWKISH_TERMS | {"above"},
                self.DOVISH_TERMS | {"below"},
            )
        return (
            self.POSITIVE_TERMS | self.HAWKISH_TERMS,
            self.NEGATIVE_TERMS | self.DOVISH_TERMS,
        )
