export type CatalystCandidateType = "scheduled_event" | "headline" | "platform_signal";
export type RelatedMarketStatus = "normal" | "possibly_lagging" | "divergent";
export type MoveDirection = "up" | "down" | "flat";
export type VisibleMarketType = "deadline_probability" | "threshold_price" | "ladder_threshold" | "unknown_visible";

export interface MarketClickContext {
  marketId: string;
  marketTitle: string;
  marketQuestion: string;
  marketSubtitle?: string;
  marketRulesPrimary?: string;
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

export interface EvidenceSource {
  title: string;
  url: string;
  source: string;
  snippet?: string;
  publishedAt?: string;
}

export interface SynthesizedCatalyst {
  summary: string;
  confidence: number;
  synthesizedAt: string;
}

export interface VisibleStatItem {
  label: string;
  value: string;
  source: "headline" | "legend" | "chart_label" | "contract_row" | "tooltip" | "generic";
  priority: number;
  confidence?: number;
}

export interface VisibleMoveSummary {
  marketType: VisibleMarketType;
  headlineValue?: string;
  headlineDelta?: string;
  asOf?: string;
  stats: VisibleStatItem[];
}

export interface AttributionResponse {
  primaryMarket: MarketClickContext;
  moveSummary: MoveSummary;
  topCatalyst?: CatalystCandidate;
  alternativeCatalysts: CatalystCandidate[];
  confidence: number;
  dataQuality: number;
  evidence: CatalystCandidate[];
  relatedMarkets: RelatedMarket[];
  synthesizedCatalyst?: SynthesizedCatalyst;
  synthesizedEvidence?: EvidenceSource[];
}

export interface AttributionSynthesisResponse {
  synthesizedCatalyst?: SynthesizedCatalyst;
  synthesizedEvidence: EvidenceSource[];
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
    (candidate.marketSubtitle === undefined || typeof candidate.marketSubtitle === "string") &&
    (candidate.marketRulesPrimary === undefined || typeof candidate.marketRulesPrimary === "string") &&
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

function isEvidenceSource(value: unknown): value is EvidenceSource {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.title === "string" &&
    typeof candidate.url === "string" &&
    typeof candidate.source === "string"
  );
}

function isSynthesizedCatalyst(value: unknown): value is SynthesizedCatalyst {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.summary === "string" &&
    typeof candidate.confidence === "number" &&
    typeof candidate.synthesizedAt === "string"
  );
}

export function isAttributionSynthesisResponse(value: unknown): value is AttributionSynthesisResponse {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  return (
    (candidate.synthesizedCatalyst === undefined ||
      candidate.synthesizedCatalyst === null ||
      isSynthesizedCatalyst(candidate.synthesizedCatalyst)) &&
    Array.isArray(candidate.synthesizedEvidence) &&
    candidate.synthesizedEvidence.every(isEvidenceSource)
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
    typeof candidate.dataQuality === "number" &&
    Array.isArray(candidate.evidence) &&
    candidate.evidence.every(isCatalystCandidate) &&
    Array.isArray(candidate.relatedMarkets) &&
    candidate.relatedMarkets.every(isRelatedMarket) &&
    (candidate.synthesizedCatalyst === undefined ||
      candidate.synthesizedCatalyst === null ||
      isSynthesizedCatalyst(candidate.synthesizedCatalyst)) &&
    (candidate.synthesizedEvidence === undefined ||
      (Array.isArray(candidate.synthesizedEvidence) &&
        candidate.synthesizedEvidence.every(isEvidenceSource)))
  );
}

export function clampScore(score: number | undefined): number | undefined {
  if (score === undefined || Number.isNaN(score)) {
    return undefined;
  }

  return Math.max(0, Math.min(1, score));
}
