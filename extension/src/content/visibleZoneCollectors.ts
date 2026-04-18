export type VisibleZone = "header" | "legend" | "chart_label" | "contract_row" | "tooltip" | "unknown";

export interface ZoneTextSnapshot {
  header: string[];
  legend: string[];
  chartLabel: string[];
  contractRow: string[];
  tooltip: string[];
  unknown: string[];
}

const MAX_LINE_LENGTH = 180;
const MAX_ELEMENTS_PER_ZONE = 220;

function normalizeText(value: string | null | undefined): string | undefined {
  const normalized = value?.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return undefined;
  }

  return normalized.length > MAX_LINE_LENGTH ? normalized.slice(0, MAX_LINE_LENGTH) : normalized;
}

function dedupe(lines: string[]): string[] {
  return [...new Set(lines.map((line) => normalizeText(line)).filter((line): line is string => Boolean(line)))];
}

function isVisibleElement(element: Element): boolean {
  const rect = element.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) {
    return false;
  }

  const style = window.getComputedStyle(element);
  return style.display !== "none" && style.visibility !== "hidden" && style.opacity !== "0";
}

function collectSelectorText(selectors: string[]): string[] {
  const lines: string[] = [];
  const visited = new Set<Element>();

  for (const selector of selectors) {
    const matches = Array.from(document.querySelectorAll(selector)).slice(0, MAX_ELEMENTS_PER_ZONE);
    for (const element of matches) {
      if (visited.has(element) || !isVisibleElement(element)) {
        continue;
      }

      visited.add(element);
      const text = normalizeText(element.textContent);
      if (text) {
        lines.push(text);
      }
    }
  }

  return dedupe(lines);
}

function collectUnknownText(): string[] {
  return collectSelectorText(["h1", "h2", "h3", "p", "span", "div", "button", "time", "text", "tspan"]);
}

export function collectZoneTextSnapshotFromDom(): ZoneTextSnapshot {
  return {
    header: collectSelectorText(["h1", "main h1", "[class*='price' i]", "[class*='updated' i]"]),
    legend: collectSelectorText(["[class*='legend' i]", "[class*='series' i]", "svg text", "tspan"]),
    chartLabel: collectSelectorText(["[class*='chart' i] text", "[class*='chart' i] tspan", "[class*='history' i] text"]),
    contractRow: collectSelectorText([
      "[class*='contract' i]",
      "[class*='outcome' i]",
      "[class*='row' i]",
      "button",
      "[class*='yes' i]",
      "[class*='no' i]",
    ]),
    tooltip: collectSelectorText(["[role='tooltip']", "[class*='tooltip' i]", "[data-testid*='tooltip' i]"]),
    unknown: collectUnknownText(),
  };
}

export function collectZoneTextSnapshotFromLines(lines: string[]): ZoneTextSnapshot {
  const normalized = dedupe(lines);
  return {
    header: normalized,
    legend: normalized,
    chartLabel: normalized,
    contractRow: normalized,
    tooltip: normalized,
    unknown: normalized,
  };
}
