import type { AttributionResponse, MarketClickContext, MoveDirection } from "../contracts";
import { mockMarketClickContext } from "./mockMarketClickContext";

const baseResponse: AttributionResponse = {
  primaryMarket: mockMarketClickContext,
  moveSummary: {
    moveMagnitude: 0.19,
    moveDirection: "up",
    jumpScore: 0.78,
  },
  topCatalyst: {
    id: "headline-cpi-preview-1",
    type: "headline",
    title: "Fed speakers and fresh services inflation commentary pushed rate-cut expectations lower",
    timestamp: "2026-04-18T13:24:00Z",
    source: "Demo Headlines Fixture",
    snippet: "Several macro desks highlighted sticky services inflation and a less dovish path for cuts.",
    url: "https://example.com/demo/fed-services-inflation",
    semanticScore: 0.84,
    timeScore: 0.93,
    importanceScore: 0.77,
    totalScore: 0.85,
  },
  alternativeCatalysts: [
    {
      id: "event-fed-minutes",
      type: "scheduled_event",
      title: "FOMC minutes release window",
      timestamp: "2026-04-18T13:00:00Z",
      source: "Macro Calendar Fixture",
      snippet: "Scheduled macro event worth checking for rate and inflation markets.",
      totalScore: 0.73,
    },
    {
      id: "headline-energy",
      type: "headline",
      title: "Crude oil rebound raised near-term inflation sensitivity",
      timestamp: "2026-04-18T13:18:00Z",
      source: "Demo Headlines Fixture",
      snippet: "Energy traders pushed front-month crude higher, supporting inflation-sensitive contracts.",
      totalScore: 0.68,
    },
  ],
  confidence: 0.74,
  dataQuality: 0.56,
  evidence: [
    {
      id: "headline-cpi-preview-1",
      type: "headline",
      title: "Fed speakers and fresh services inflation commentary pushed rate-cut expectations lower",
      timestamp: "2026-04-18T13:24:00Z",
      source: "Demo Headlines Fixture",
      snippet: "Several macro desks highlighted sticky services inflation and a less dovish path for cuts.",
      totalScore: 0.85,
    },
    {
      id: "event-fed-minutes",
      type: "scheduled_event",
      title: "FOMC minutes release window",
      timestamp: "2026-04-18T13:00:00Z",
      source: "Macro Calendar Fixture",
      snippet: "Scheduled macro event worth checking for rate and inflation markets.",
      totalScore: 0.73,
    },
  ],
  relatedMarkets: [
    {
      marketId: "KXRATES-FEDCUT-SEP2026",
      title: "Will the Fed cut by September 2026?",
      relationTypes: ["macro_cluster", "semantic_similarity", "historical_comovement"],
      relationStrength: 0.82,
      expectedReactionScore: 0.76,
      residualZscore: 0.4,
      status: "normal",
      note: "Rates-sensitive market that usually reacts alongside inflation repricing.",
    },
    {
      marketId: "KXGOLD-ABOVE3400-JUN2026",
      title: "Will gold trade above $3,400 by June 2026?",
      relationTypes: ["inflation_proxy", "cross_asset_proxy"],
      relationStrength: 0.63,
      expectedReactionScore: 0.58,
      residualZscore: 2.1,
      status: "possibly_lagging",
      note: "Worth checking: proxy market has weaker-than-expected follow-through for the same inflation shock.",
    },
  ],
};

function getMoveDirection(priceBefore: number | undefined, priceAfter: number | undefined): MoveDirection {
  if (priceBefore === undefined || priceAfter === undefined) {
    return baseResponse.moveSummary.moveDirection;
  }

  if (priceAfter > priceBefore) {
    return "up";
  }

  if (priceAfter < priceBefore) {
    return "down";
  }

  return "flat";
}

function getMoveMagnitude(priceBefore: number | undefined, priceAfter: number | undefined): number {
  if (priceBefore === undefined || priceAfter === undefined) {
    return baseResponse.moveSummary.moveMagnitude;
  }

  return Number(Math.abs(priceAfter - priceBefore).toFixed(2));
}

export function buildMockAttributionResponse(
  context: MarketClickContext = mockMarketClickContext,
): AttributionResponse {
  const moveMagnitude = getMoveMagnitude(context.priceBefore, context.priceAfter);
  const moveDirection = getMoveDirection(context.priceBefore, context.priceAfter);

  return {
    ...baseResponse,
    primaryMarket: context,
    moveSummary: {
      moveMagnitude,
      moveDirection,
      jumpScore: moveMagnitude > 0 ? 0.78 : 0.32,
    },
    dataQuality:
      context.priceBefore !== undefined && context.priceAfter !== undefined
        ? 0.56
        : context.clickedPrice !== undefined
          ? 0.34
          : baseResponse.dataQuality,
    topCatalyst: baseResponse.topCatalyst
      ? {
          ...baseResponse.topCatalyst,
          title: `${context.marketTitle}: likely macro catalyst`,
        }
      : undefined,
    evidence: baseResponse.evidence.map((item) => ({ ...item })),
    alternativeCatalysts: baseResponse.alternativeCatalysts.map((item) => ({ ...item })),
    relatedMarkets: baseResponse.relatedMarkets.map((item) => ({ ...item })),
  };
}

export const mockAttributionResponse = buildMockAttributionResponse();
