import type { VisibleMarketType, VisibleMoveSummary, VisibleStatItem } from "../shared/contracts";
import { filterMeaningfulStats } from "./fieldValidation";
import { classifyVisibleMarketType } from "./marketTypeClassifier";
import { collectZoneTextSnapshotFromDom, collectZoneTextSnapshotFromLines, type ZoneTextSnapshot } from "./visibleZoneCollectors";

interface VisibleSummaryInput {
  marketTitle: string;
  marketQuestion: string;
  clickedTimestamp: string;
  textLines: string[];
}

const DEADLINE_VALUE_PATTERN = /\b(Before\s+[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\s+(\d{1,3}(?:\.\d+)?)%/i;
const THRESHOLD_PATTERN = /\b(Above|Below)\s+(-?\d+(?:\.\d+)?)\b/i;
const YES_NO_PATTERN = /\b(Yes|No)\s+(\d+(?:\.\d+)?)\s?(?:¢|c)/i;
const CURRENCY_PATTERN = /\$\s?\d[\d,]*(?:\.\d+)?\b/;
const CENTS_PATTERN = /\b\d{1,3}(?:\.\d+)?\s?(?:¢|c)\b/i;
const ARROW_DELTA_PATTERN = /([▲▼])\s*(\d+(?:\.\d+)?)%/;
const SIGNED_DELTA_PATTERN = /([+-]\d+(?:\.\d+)?)%/;
const AS_OF_PATTERN = /\bupdated\s+.+/i;
const TIMESTAMP_PATTERN = /\b\d{1,2}\/\d{1,2}\/\d{4},\s+\d{1,2}:\d{2}\s*(?:AM|PM)\b/i;

function flattenZones(zones: ZoneTextSnapshot): string[] {
  return [...zones.header, ...zones.legend, ...zones.chartLabel, ...zones.contractRow, ...zones.tooltip, ...zones.unknown];
}

function findAsOf(zones: ZoneTextSnapshot, fallback: string): string {
  for (const line of flattenZones(zones)) {
    if (AS_OF_PATTERN.test(line)) {
      return line;
    }

    const timestampMatch = line.match(TIMESTAMP_PATTERN);
    if (timestampMatch) {
      return timestampMatch[0];
    }
  }

  return fallback;
}

function parseHeadline(zones: ZoneTextSnapshot): { headlineValue?: string; headlineDelta?: string } {
  const headerLines = [...zones.header, ...zones.tooltip];

  for (const line of headerLines) {
    const value = line.match(CURRENCY_PATTERN)?.[0] ?? line.match(CENTS_PATTERN)?.[0]?.replace(/\s+/g, "");
    if (!value) {
      continue;
    }

    const arrow = line.match(ARROW_DELTA_PATTERN);
    const signed = line.match(SIGNED_DELTA_PATTERN);
    const headlineDelta = arrow ? `${arrow[1]} ${arrow[2]}%` : signed ? signed[1] : undefined;

    if (headlineDelta) {
      return {
        headlineValue: value.replace(/\s+/g, ""),
        headlineDelta,
      };
    }
  }

  for (const line of headerLines) {
    const value = line.match(CURRENCY_PATTERN)?.[0] ?? line.match(CENTS_PATTERN)?.[0]?.replace(/\s+/g, "");
    if (value) {
      return { headlineValue: value.replace(/\s+/g, "") };
    }
  }

  return {};
}

function extractDeadlineStats(zones: ZoneTextSnapshot): VisibleStatItem[] {
  const stats: VisibleStatItem[] = [];
  const seen = new Set<string>();
  const lines = [...zones.contractRow, ...zones.legend, ...zones.tooltip];

  for (const line of lines) {
    const match = line.match(DEADLINE_VALUE_PATTERN);
    if (!match) {
      continue;
    }

    const label = match[1];
    const value = `${match[2]}%`;
    const key = `${label}|${value}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    stats.push({
      label,
      value,
      source: zones.contractRow.includes(line) ? "contract_row" : "legend",
      priority: 100 - stats.length,
      confidence: 0.9,
    });
  }

  return stats.slice(0, 4);
}

function extractThresholdStats(zones: ZoneTextSnapshot): VisibleStatItem[] {
  const stats: VisibleStatItem[] = [];
  const seen = new Set<string>();
  const lines = [...zones.chartLabel, ...zones.contractRow, ...zones.legend];

  for (const line of lines) {
    const threshold = line.match(THRESHOLD_PATTERN);
    if (!threshold) {
      continue;
    }

    const value = `${threshold[1]} ${threshold[2]}`;
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    stats.push({
      label: "Contract",
      value,
      source: zones.contractRow.includes(line) ? "contract_row" : "chart_label",
      priority: 95 - stats.length,
      confidence: 0.88,
    });
  }

  return stats.slice(0, 3);
}

function extractLadderStats(zones: ZoneTextSnapshot): VisibleStatItem[] {
  const stats = extractThresholdStats(zones);
  const seen = new Set(stats.map((item) => `${item.label}|${item.value}`));

  for (const line of zones.contractRow) {
    const matches = [...line.matchAll(new RegExp(YES_NO_PATTERN.source, "gi"))];
    for (const match of matches) {
      const side = match[1];
      const price = match[2];
      if (!side || !price) {
        continue;
      }

      const label = `${side[0].toUpperCase()}${side.slice(1).toLowerCase()} price`;
      const value = `${price}c`;
      const key = `${label}|${value}`;
      if (seen.has(key)) {
        continue;
      }

      seen.add(key);
      stats.push({
        label,
        value,
        source: "contract_row",
        priority: 85 - stats.length,
        confidence: 0.86,
      });
    }
  }

  return stats.slice(0, 5);
}

function inferContractFromQuestion(marketQuestion: string): string | undefined {
  const match = marketQuestion.match(THRESHOLD_PATTERN);
  if (!match) {
    return undefined;
  }

  return `${match[1]} ${match[2]}`;
}

function withMeaningfulFallback(
  stats: VisibleStatItem[],
  marketType: VisibleMarketType,
  marketTitle: string,
  marketQuestion: string,
): VisibleStatItem[] {
  if (stats.length > 0) {
    return stats;
  }

  const fallback: VisibleStatItem[] = [
    { label: "Market", value: marketTitle, source: "generic", priority: 10, confidence: 1 },
  ];

  const contract = inferContractFromQuestion(marketQuestion);
  if (contract && (marketType === "threshold_price" || marketType === "ladder_threshold")) {
    fallback.push({
      label: "Contract",
      value: contract,
      source: "generic",
      priority: 9,
      confidence: 0.55,
    });
  } else {
    fallback.push({
      label: "Question",
      value: marketQuestion,
      source: "generic",
      priority: 9,
      confidence: 0.75,
    });
  }

  return fallback;
}

function extractTypeSpecificStats(marketType: VisibleMarketType, zones: ZoneTextSnapshot): VisibleStatItem[] {
  switch (marketType) {
    case "deadline_probability":
      return extractDeadlineStats(zones);
    case "ladder_threshold":
      return extractLadderStats(zones);
    case "threshold_price":
      return extractThresholdStats(zones);
    default:
      return [];
  }
}

export function detectVisibleMarketType(marketQuestion: string, textLines: string[]): VisibleMarketType {
  const zones = collectZoneTextSnapshotFromLines(textLines);
  return classifyVisibleMarketType(marketQuestion, zones).marketType;
}

export function extractVisibleMoveSummaryFromLines(input: VisibleSummaryInput): VisibleMoveSummary {
  const zones = collectZoneTextSnapshotFromLines(input.textLines);
  return extractVisibleMoveSummary({
    marketTitle: input.marketTitle,
    marketQuestion: input.marketQuestion,
    clickedTimestamp: input.clickedTimestamp,
    zones,
  });
}

function extractVisibleMoveSummary({
  marketTitle,
  marketQuestion,
  clickedTimestamp,
  zones,
}: {
  marketTitle: string;
  marketQuestion: string;
  clickedTimestamp: string;
  zones: ZoneTextSnapshot;
}): VisibleMoveSummary {
  const classification = classifyVisibleMarketType(marketQuestion, zones);
  const headline = parseHeadline(zones);
  const asOf = findAsOf(zones, clickedTimestamp);
  const rawStats = extractTypeSpecificStats(classification.marketType, zones);
  const meaningfulStats = filterMeaningfulStats(rawStats).slice(0, 6);

  return {
    marketType: classification.marketType,
    headlineValue: headline.headlineValue,
    headlineDelta: headline.headlineDelta,
    asOf,
    stats: withMeaningfulFallback(meaningfulStats, classification.marketType, marketTitle, marketQuestion),
  };
}

export function extractVisibleMoveSummaryFromDom(
  marketTitle: string,
  marketQuestion: string,
  clickedTimestamp: string,
): VisibleMoveSummary {
  const zones = collectZoneTextSnapshotFromDom();
  return extractVisibleMoveSummary({
    marketTitle,
    marketQuestion,
    clickedTimestamp,
    zones,
  });
}
