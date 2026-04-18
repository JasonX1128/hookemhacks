from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


CatalystCandidateType = Literal["scheduled_event", "headline", "platform_signal"]
RelatedMarketStatus = Literal["normal", "possibly_lagging", "divergent"]
MoveDirection = Literal["up", "down", "flat"]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MarketClickContext(ContractModel):
    marketId: str = Field(
        ...,
        description="Kalshi market ticker or a best-effort page identifier.",
    )
    marketTitle: str
    marketQuestion: str
    clickedTimestamp: str
    clickedPrice: float | None = None
    windowStart: str
    windowEnd: str
    priceBefore: float | None = None
    priceAfter: float | None = None


class RetrievedCatalystCandidate(ContractModel):
    id: str
    type: CatalystCandidateType
    title: str
    timestamp: str
    source: str
    snippet: str | None = None
    url: str | None = None
    importance: float = 0.5
    keywords: list[str] = Field(default_factory=list)
    directionalHint: MoveDirection | None = None


class CatalystCandidate(ContractModel):
    id: str
    type: CatalystCandidateType
    title: str
    timestamp: str
    source: str
    snippet: str | None = None
    url: str | None = None
    semanticScore: float | None = None
    timeScore: float | None = None
    importanceScore: float | None = None
    totalScore: float | None = None


class RelatedMarket(ContractModel):
    marketId: str
    title: str
    relationTypes: list[str]
    relationStrength: float
    expectedReactionScore: float | None = None
    residualZscore: float | None = None
    status: RelatedMarketStatus | None = None
    note: str | None = None


class MoveSummary(ContractModel):
    moveMagnitude: float
    moveDirection: MoveDirection
    jumpScore: float


class EvidenceSource(ContractModel):
    title: str
    url: str
    source: str
    snippet: str | None = None
    publishedAt: str | None = None


class SynthesizedCatalyst(ContractModel):
    summary: str
    confidence: float
    synthesizedAt: str


class AttributionResponse(ContractModel):
    primaryMarket: MarketClickContext
    moveSummary: MoveSummary
    topCatalyst: CatalystCandidate | None = None
    alternativeCatalysts: list[CatalystCandidate]
    confidence: float
    evidence: list[CatalystCandidate]
    relatedMarkets: list[RelatedMarket]
    synthesizedCatalyst: SynthesizedCatalyst | None = None
    synthesizedEvidence: list[EvidenceSource] = Field(default_factory=list)


class HealthResponse(ContractModel):
    status: str
    service: str
    mockMode: bool
    environment: str
