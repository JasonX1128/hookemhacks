import { describe, expect, it } from "vitest";

import { coerceAttributionResponse, normalizeEndpointUrl } from "../src/shared/api";
import { buildMockAttributionResponse } from "../src/shared/fixtures/mockAttributionResponse";

describe("buildMockAttributionResponse", () => {
  it("uses the provided market context and derives a move summary", () => {
    const response = buildMockAttributionResponse({
      marketId: "demo-market",
      marketTitle: "Will demo happen?",
      marketQuestion: "Will demo happen?",
      clickedTimestamp: "2026-04-18T13:30:00Z",
      clickedPrice: 0.58,
      windowStart: "2026-04-18T13:00:00Z",
      windowEnd: "2026-04-18T14:00:00Z",
      priceBefore: 0.42,
      priceAfter: 0.58,
    });

    expect(response.primaryMarket.marketId).toBe("demo-market");
    expect(response.moveSummary.moveDirection).toBe("up");
    expect(response.moveSummary.moveMagnitude).toBe(0.16);
  });
});

describe("normalizeEndpointUrl", () => {
  it("falls back to the default localhost endpoint when input is invalid", () => {
    expect(normalizeEndpointUrl("not-a-url")).toBe("http://127.0.0.1:8000/attribute_move");
  });

  it("keeps valid URLs intact", () => {
    expect(normalizeEndpointUrl("http://localhost:9000/attribute_move")).toBe(
      "http://localhost:9000/attribute_move",
    );
  });
});

describe("coerceAttributionResponse", () => {
  it("fills missing backend fields with mock defaults for the current context", () => {
    const context = {
      marketId: "demo-market",
      marketTitle: "Will demo happen?",
      marketQuestion: "Will demo happen?",
      clickedTimestamp: "2026-04-18T13:30:00Z",
      clickedPrice: 0.58,
      windowStart: "2026-04-18T13:00:00Z",
      windowEnd: "2026-04-18T14:00:00Z",
      priceBefore: 0.42,
      priceAfter: 0.58,
    } as const;

    const response = coerceAttributionResponse(
      {
        primaryMarket: context,
        moveSummary: {
          moveMagnitude: 0.16,
          moveDirection: "up",
          jumpScore: 0.77,
        },
        confidence: 0.66,
        evidence: [],
      },
      context,
    );

    expect(response).not.toBeNull();
    expect(response?.primaryMarket.marketId).toBe("demo-market");
    expect(response?.moveSummary.jumpScore).toBe(0.77);
    expect(response?.relatedMarkets.length).toBeGreaterThan(0);
  });

  it("preserves intentional empty arrays from the backend", () => {
    const context = {
      marketId: "demo-market",
      marketTitle: "Will demo happen?",
      marketQuestion: "Will demo happen?",
      clickedTimestamp: "2026-04-18T13:30:00Z",
      clickedPrice: 0.58,
      windowStart: "2026-04-18T13:00:00Z",
      windowEnd: "2026-04-18T14:00:00Z",
      priceBefore: 0.42,
      priceAfter: 0.58,
    } as const;

    const response = coerceAttributionResponse(
      {
        primaryMarket: context,
        moveSummary: {
          moveMagnitude: 0.16,
          moveDirection: "up",
          jumpScore: 0.77,
        },
        topCatalyst: null,
        alternativeCatalysts: [],
        confidence: 0.66,
        evidence: [],
        relatedMarkets: [],
      },
      context,
    );

    expect(response).not.toBeNull();
    expect(response?.topCatalyst).toBeUndefined();
    expect(response?.alternativeCatalysts).toEqual([]);
    expect(response?.evidence).toEqual([]);
    expect(response?.relatedMarkets).toEqual([]);
  });
});
