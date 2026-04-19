import { describe, expect, it } from "vitest";

import { detectVisibleMarketType, extractVisibleMoveSummaryFromLines } from "../src/content/visibleMoveSummary";
import { deadlineSampleLines, ladderSampleLines, thresholdSampleLines } from "./fixtures/visibleMarketSamples";

describe("detectVisibleMarketType", () => {
  it("detects deadline probability markets", () => {
    const result = detectVisibleMarketType("When will traffic return to normal?", deadlineSampleLines);

    expect(result).toBe("deadline_probability");
  });

  it("detects threshold markets", () => {
    const result = detectVisibleMarketType("Oil Price (WTI) on Monday?", thresholdSampleLines);
    expect(result).toBe("threshold_price");
  });

  it("detects ladder threshold markets", () => {
    const result = detectVisibleMarketType("US gas prices this week", ladderSampleLines);
    expect(result).toBe("ladder_threshold");
  });
});

describe("extractVisibleMoveSummaryFromLines", () => {
  it("extracts deadline legend rows as stats", () => {
    const summary = extractVisibleMoveSummaryFromLines({
      marketTitle: "CPI market",
      marketQuestion: "Will CPI release come before Jul 1, 2026?",
      clickedTimestamp: "2026-04-18T10:00:00Z",
      textLines: deadlineSampleLines,
    });

    expect(summary.marketType).toBe("deadline_probability");
    expect(summary.stats.some((item) => item.label === "Before Jul 1, 2026" && item.value === "58.3%")).toBe(true);
  });

  it("extracts threshold contract + headline values", () => {
    const summary = extractVisibleMoveSummaryFromLines({
      marketTitle: "Gold market",
      marketQuestion: "Will price end above 91?",
      clickedTimestamp: "2026-04-18T10:00:00Z",
      textLines: thresholdSampleLines,
    });

    expect(summary.marketType).toBe("threshold_price");
    expect(summary.headlineValue).toBe("$93.66");
    expect(summary.headlineDelta).toBe("▲ 0.5%");
    expect(summary.stats.some((item) => item.label === "Contract" && item.value === "Above 92")).toBe(true);
  });

  it("extracts ladder yes/no pricing rows", () => {
    const summary = extractVisibleMoveSummaryFromLines({
      marketTitle: "US gas prices this week",
      marketQuestion: "US gas prices this week",
      clickedTimestamp: "2026-04-18T10:00:00Z",
      textLines: ladderSampleLines,
    });

    expect(summary.marketType).toBe("ladder_threshold");
    expect(summary.stats.some((item) => item.label === "Yes price" && item.value === "90c")).toBe(true);
    expect(summary.stats.some((item) => item.label === "No price" && item.value === "16c")).toBe(true);
  });

  it("always returns non-empty stats", () => {
    const summary = extractVisibleMoveSummaryFromLines({
      marketTitle: "Fallback market",
      marketQuestion: "Will something happen?",
      clickedTimestamp: "2026-04-18T10:00:00Z",
      textLines: [],
    });

    expect(summary.stats.length).toBeGreaterThan(0);
    expect(summary.stats[0]?.label).toBe("Market");
  });

  it("does not emit unlabeled generic displayed fields", () => {
    const summary = extractVisibleMoveSummaryFromLines({
      marketTitle: "Sample market",
      marketQuestion: "Will sample happen?",
      clickedTimestamp: "2026-04-18T10:00:00Z",
      textLines: ["7%", "180%", "16c"],
    });

    expect(summary.stats.some((item) => item.label.startsWith("Displayed"))).toBe(false);
  });

  it("falls back to the market title when the market question is missing", () => {
    const summary = extractVisibleMoveSummaryFromLines({
      marketTitle: "Fallback market",
      clickedTimestamp: "2026-04-18T10:00:00Z",
      textLines: [],
    });

    expect(summary.stats.some((item) => item.label === "Question" && item.value === "Fallback market")).toBe(true);
  });
});
