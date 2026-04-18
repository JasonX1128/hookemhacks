import type { VisibleMarketType } from "../shared/contracts";
import type { ZoneTextSnapshot } from "./visibleZoneCollectors";

export interface MarketTypeClassification {
  marketType: VisibleMarketType;
  confidence: number;
  evidence: string[];
}

function countMatches(lines: string[], pattern: RegExp): number {
  let count = 0;
  for (const line of lines) {
    if (pattern.test(line)) {
      count += 1;
    }
  }

  return count;
}

export function classifyVisibleMarketType(question: string, zones: ZoneTextSnapshot): MarketTypeClassification {
  const evidence: string[] = [];
  const normalizedQuestion = question.toLowerCase();

  const deadlineRowCount = countMatches(
    [...zones.contractRow, ...zones.legend],
    /\bBefore\s+[A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\b/i,
  );
  const thresholdRowCount = countMatches([...zones.contractRow, ...zones.chartLabel], /\b(Above|Below)\s+-?\d+(?:\.\d+)?\b/i);
  const yesNoPriceCount = countMatches(zones.contractRow, /\b(Yes|No)\s+\d+(?:\.\d+)?\s?(?:¢|c)/i);

  if (deadlineRowCount >= 2 || (/\b(before|by|between)\b/.test(normalizedQuestion) && deadlineRowCount >= 1)) {
    evidence.push(`deadlineRows:${deadlineRowCount}`);
    return {
      marketType: "deadline_probability",
      confidence: Math.min(1, 0.6 + deadlineRowCount * 0.1),
      evidence,
    };
  }

  if (thresholdRowCount >= 2 && yesNoPriceCount >= 1) {
    evidence.push(`thresholdRows:${thresholdRowCount}`, `yesNoRows:${yesNoPriceCount}`);
    return {
      marketType: "ladder_threshold",
      confidence: Math.min(1, 0.65 + thresholdRowCount * 0.08),
      evidence,
    };
  }

  if (thresholdRowCount >= 1 || /\b(above|below|over|under)\b/.test(normalizedQuestion)) {
    evidence.push(`thresholdRows:${thresholdRowCount}`);
    return {
      marketType: "threshold_price",
      confidence: Math.min(1, 0.55 + thresholdRowCount * 0.1),
      evidence,
    };
  }

  return {
    marketType: "unknown_visible",
    confidence: 0.25,
    evidence: ["fallback:unknown"],
  };
}
