import { describe, expect, it } from "vitest";

import { isAttributionResponse, isMarketClickContext } from "../src/shared/contracts";
import { mockAttributionResponse } from "../src/shared/fixtures/mockAttributionResponse";
import { mockMarketClickContext } from "../src/shared/fixtures/mockMarketClickContext";
import { estimateRatioFromClientX, interpolateTimestamp } from "../src/content/chartCapture";

describe("shared contracts", () => {
  it("recognizes a valid MarketClickContext fixture", () => {
    expect(isMarketClickContext(mockMarketClickContext)).toBe(true);
  });

  it("recognizes a valid AttributionResponse fixture", () => {
    expect(isAttributionResponse(mockAttributionResponse)).toBe(true);
  });
});

describe("chart capture helpers", () => {
  it("clamps the x ratio inside the chart bounds", () => {
    expect(estimateRatioFromClientX(50, 0, 100)).toBe(0.5);
    expect(estimateRatioFromClientX(-10, 0, 100)).toBe(0);
    expect(estimateRatioFromClientX(150, 0, 100)).toBe(1);
  });

  it("interpolates a timestamp within the provided range", () => {
    expect(interpolateTimestamp("2026-04-18T13:00:00Z", "2026-04-18T14:00:00Z", 0.5)).toBe(
      "2026-04-18T13:30:00.000Z",
    );
  });
});
