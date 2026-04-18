export type CatalystCandidateType = "scheduled_event" | "headline" | "platform_signal";
export type RelatedMarketStatus = "normal" | "possibly_lagging" | "divergent";
export type MoveDirection = "up" | "down" | "flat";

export interface MarketClickContext {
  marketId: string;
  marketTitle: string;
  marketQuestion: string;
  clickedTimestamp: string;
  clickedPrice?: number;
  windowStart: string;
  windowEnd: string;
  priceBefore?: number;
  priceAfter?: number;
}

export interface CatalystCandidate {
  id: string;
  type: CatalystCandidateType;
  title: string;
  timestamp: string;
  source: string;
  snippet?: string;
  url?: string;
  semanticScore?: number;
  timeScore?: number;
  importanceScore?: number;
  totalScore?: number;
}

export interface RelatedMarket {
  marketId: string;
  title: string;
  relationTypes: string[];
  relationStrength: number;
  expectedReactionScore?: number;
  residualZscore?: number;
  status?: RelatedMarketStatus;
  note?: string;
}

export interface MoveSummary {
  moveMagnitude: number;
  moveDirection: MoveDirection;
  jumpScore: number;
}

export interface AttributionResponse {
  primaryMarket: MarketClickContext;
  moveSummary: MoveSummary;
  topCatalyst?: CatalystCandidate;
  alternativeCatalysts: CatalystCandidate[];
  confidence: number;
  evidence: CatalystCandidate[];
  relatedMarkets: RelatedMarket[];
}

export function isMarketClickContext(value: unknown): value is MarketClickContext {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.marketId === "string" &&
    typeof candidate.marketTitle === "string" &&
    typeof candidate.marketQuestion === "string" &&
    typeof candidate.clickedTimestamp === "string" &&
    typeof candidate.windowStart === "string" &&
    typeof candidate.windowEnd === "string"
  );
}

function isCatalystCandidate(value: unknown): value is CatalystCandidate {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.id === "string" &&
    typeof candidate.type === "string" &&
    typeof candidate.title === "string" &&
    typeof candidate.timestamp === "string" &&
    typeof candidate.source === "string"
  );
}

function isRelatedMarket(value: unknown): value is RelatedMarket {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.marketId === "string" &&
    typeof candidate.title === "string" &&
    Array.isArray(candidate.relationTypes) &&
    candidate.relationTypes.every((relation) => typeof relation === "string") &&
    typeof candidate.relationStrength === "number"
  );
}

function isMoveSummary(value: unknown): value is MoveSummary {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.moveMagnitude === "number" &&
    typeof candidate.moveDirection === "string" &&
    typeof candidate.jumpScore === "number"
  );
}

export function isAttributionResponse(value: unknown): value is AttributionResponse {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    isMarketClickContext(candidate.primaryMarket) &&
    isMoveSummary(candidate.moveSummary) &&
    (candidate.topCatalyst === undefined ||
      candidate.topCatalyst === null ||
      isCatalystCandidate(candidate.topCatalyst)) &&
    Array.isArray(candidate.alternativeCatalysts) &&
    candidate.alternativeCatalysts.every(isCatalystCandidate) &&
    typeof candidate.confidence === "number" &&
    Array.isArray(candidate.evidence) &&
    candidate.evidence.every(isCatalystCandidate) &&
    Array.isArray(candidate.relatedMarkets) &&
    candidate.relatedMarkets.every(isRelatedMarket)
  );
}

export function clampScore(score: number | undefined): number | undefined {
  if (score === undefined || Number.isNaN(score)) {
    return undefined;
  }

  return Math.max(0, Math.min(1, score));
}
