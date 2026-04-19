import { describe, expect, it } from "vitest";

import { classifyVisibleMarketType } from "../src/content/marketTypeClassifier";
import { collectZoneTextSnapshotFromLines } from "../src/content/visibleZoneCollectors";
import { deadlineSampleLines, ladderSampleLines, thresholdSampleLines } from "./fixtures/visibleMarketSamples";

describe("classifyVisibleMarketType", () => {
  it("classifies deadline markets with high confidence", () => {
    const result = classifyVisibleMarketType(
      "When will traffic return to normal?",
      collectZoneTextSnapshotFromLines(deadlineSampleLines),
    );

    expect(result.marketType).toBe("deadline_probability");
    expect(result.confidence).toBeGreaterThan(0.7);
  });

  it("classifies threshold markets", () => {
    const result = classifyVisibleMarketType(
      "Oil Price (WTI) on Monday?",
      collectZoneTextSnapshotFromLines(thresholdSampleLines),
    );

    expect(result.marketType).toBe("threshold_price");
  });

  it("classifies ladder markets using yes/no rows", () => {
    const result = classifyVisibleMarketType(
      "US gas prices this week",
      collectZoneTextSnapshotFromLines(ladderSampleLines),
    );

    expect(result.marketType).toBe("ladder_threshold");
    expect(result.evidence.some((item) => item.includes("yesNoRows"))).toBe(true);
  });

  it("does not throw when the market question is missing", () => {
    const result = classifyVisibleMarketType(undefined, collectZoneTextSnapshotFromLines([]));

    expect(result.marketType).toBe("unknown_visible");
  });
});
