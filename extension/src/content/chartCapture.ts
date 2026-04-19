import type { MarketClickContext } from "../shared/contracts";
import {
  findClosestChartContainer,
  findClosestChartElement,
  findLikelyChartContainer,
  findLikelyChartElement,
  isClickInsideChart,
} from "./kalshiPage";
import { extractMarketMetadata, resolveMarketMetadata } from "./metadataExtractor";

export interface ChartCaptureCallbacks {
  onContext: (context: MarketClickContext) => void | Promise<void>;
}

interface ClickSnapshot {
  clientX: number;
  clientY: number;
  target: Element | null;
}

interface TimestampLabelPoint {
  timestampMs: number;
  x: number;
}

interface PriceLabelPoint {
  price: number;
  y: number;
}

interface TimelineBounds {
  startMs: number;
  endMs: number;
  labelStepMs?: number;
}

const TOOLTIP_SELECTORS = [
  "[role='tooltip']",
  "[class*='tooltip' i]",
  "[data-testid*='tooltip' i]",
  "[aria-live='polite']",
].join(", ");

const TEXT_SELECTORS = "text, tspan, span, time, div, p";
const MAX_TEXT_ELEMENTS = 180;
const DEFAULT_HALF_WINDOW_MS = 30 * 60 * 1000;
const MIN_HALF_WINDOW_MS = 5 * 60 * 1000;
const MAX_HALF_WINDOW_MS = 12 * 60 * 60 * 1000;

export function estimateRatioFromClientX(clientX: number, left: number, width: number): number {
  if (width <= 0) {
    return 0.5;
  }

  return Math.max(0, Math.min(1, (clientX - left) / width));
}

export function interpolateTimestamp(startIso: string, endIso: string, ratio: number): string {
  const start = new Date(startIso).getTime();
  const end = new Date(endIso).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end <= start) {
    return new Date().toISOString();
  }

  const timestamp = start + (end - start) * Math.max(0, Math.min(1, ratio));
  return new Date(timestamp).toISOString();
}

function normalizeText(value: string | null | undefined): string | undefined {
  const normalized = value?.replace(/\s+/g, " ").trim();
  return normalized || undefined;
}

function parseDateFromText(value: string): string | undefined {
  const normalized = normalizeText(value);
  if (!normalized) {
    return undefined;
  }

  const referenceDate = new Date();
  const candidates = [
    normalized,
    ...normalized
      .split(/\n|[|•]/)
      .map((part) => part.trim())
      .filter(Boolean),
  ];

  for (const candidate of candidates) {
    const direct = new Date(candidate);
    if (!Number.isNaN(direct.getTime())) {
      return direct.toISOString();
    }

    const relativeCandidate = candidate
      .replace(/\btoday\b/i, referenceDate.toDateString())
      .replace(/\byesterday\b/i, new Date(referenceDate.getTime() - 24 * 60 * 60 * 1000).toDateString())
      .replace(/\btomorrow\b/i, new Date(referenceDate.getTime() + 24 * 60 * 60 * 1000).toDateString());
    const relativeDate = new Date(relativeCandidate);
    if (!Number.isNaN(relativeDate.getTime())) {
      return relativeDate.toISOString();
    }

    const monthOrNumericDatePattern =
      /\b(?:\d{1,2}\/\d{1,2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2})(?:,\s*\d{4})?\b/i;
    if (monthOrNumericDatePattern.test(candidate) && !/\b\d{4}\b/.test(candidate)) {
      const withYear = `${candidate}, ${referenceDate.getFullYear()}`;
      const datedWithYear = new Date(withYear);
      if (!Number.isNaN(datedWithYear.getTime())) {
        return datedWithYear.toISOString();
      }
    }

    const timeOnlyMatch = candidate.match(/\b(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m?\.?\b/i);
    if (timeOnlyMatch) {
      const hours = Number.parseInt(timeOnlyMatch[1], 10) % 12;
      const minutes = Number.parseInt(timeOnlyMatch[2] ?? "0", 10);
      const meridiemHours = timeOnlyMatch[3].toLowerCase() === "p" ? hours + 12 : hours;
      const parsed = new Date(referenceDate);
      parsed.setHours(meridiemHours, minutes, 0, 0);
      return parsed.toISOString();
    }
  }

  return undefined;
}

function parsePriceFromText(value: string): number | undefined {
  const normalized = normalizeText(value);
  if (!normalized) {
    return undefined;
  }

  const centOrPercentMatch = normalized.match(/\b(\d{1,3}(?:\.\d+)?)\s*(?:¢|c|cents|%)\b/i);
  if (centOrPercentMatch) {
    const raw = Number.parseFloat(centOrPercentMatch[1]);
    if (!Number.isNaN(raw)) {
      return Math.max(0, Math.min(0.99, Number((raw > 1 ? raw / 100 : raw).toFixed(2))));
    }
  }

  const probabilityMatch = normalized.match(/\b(?:yes|no|price|probability)\D{0,6}(0?\.\d{1,3})\b/i);
  if (!probabilityMatch) {
    return undefined;
  }

  const probability = Number.parseFloat(probabilityMatch[1]);
  if (Number.isNaN(probability)) {
    return undefined;
  }

  return Math.max(0, Math.min(0.99, Number(probability.toFixed(2))));
}

function isVisibleElement(element: Element): boolean {
  const rect = element.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) {
    return false;
  }

  const style = window.getComputedStyle(element);
  return style.display !== "none" && style.visibility !== "hidden" && style.opacity !== "0";
}

function collectStringsFromElement(element: Element | null): string[] {
  if (!element) {
    return [];
  }

  const strings = new Set<string>();
  const textContent = normalizeText(element.textContent);
  if (textContent) {
    strings.add(textContent);
  }

  if (element instanceof HTMLElement) {
    const innerText = normalizeText(element.innerText);
    if (innerText) {
      strings.add(innerText);
    }
  }

  for (const attribute of ["aria-label", "title", "data-value", "data-tooltip-content"]) {
    const value = normalizeText(element.getAttribute(attribute));
    if (value) {
      strings.add(value);
    }
  }

  return [...strings];
}

function collectPointElements(click: ClickSnapshot): Element[] {
  return document.elementsFromPoint(click.clientX, click.clientY).filter((element) => element instanceof Element);
}

function isRectNearChart(rect: DOMRect, chartRect: DOMRect): boolean {
  return (
    rect.right >= chartRect.left - 120 &&
    rect.left <= chartRect.right + 120 &&
    rect.bottom >= chartRect.top - 80 &&
    rect.top <= chartRect.bottom + 120
  );
}

function collectTooltipStrings(chartRect: DOMRect): string[] {
  const strings = new Set<string>();

  for (const candidate of Array.from(document.querySelectorAll(TOOLTIP_SELECTORS))) {
    if (!isVisibleElement(candidate) || !isRectNearChart(candidate.getBoundingClientRect(), chartRect)) {
      continue;
    }

    for (const text of collectStringsFromElement(candidate)) {
      strings.add(text);
    }
  }

  return [...strings];
}

function collectContextStrings(click: ClickSnapshot, chartRect: DOMRect): string[] {
  const strings = new Set<string>();

  for (const element of collectPointElements(click).slice(0, 8)) {
    let current: Element | null = element;
    let depth = 0;

    while (current && depth < 4) {
      for (const text of collectStringsFromElement(current)) {
        strings.add(text);
      }

      current = current.parentElement;
      depth += 1;
    }
  }

  for (const text of collectTooltipStrings(chartRect)) {
    strings.add(text);
  }

  return [...strings];
}

function median(values: number[]): number | undefined {
  if (!values.length) {
    return undefined;
  }

  const sorted = [...values].sort((left, right) => left - right);
  const middleIndex = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middleIndex];
  }

  return (sorted[middleIndex - 1] + sorted[middleIndex]) / 2;
}

function dedupeTimestampLabels(labels: TimestampLabelPoint[]): TimestampLabelPoint[] {
  const deduped: TimestampLabelPoint[] = [];

  for (const label of [...labels].sort((left, right) => left.x - right.x)) {
    const last = deduped.at(-1);
    if (last && Math.abs(last.x - label.x) < 8) {
      continue;
    }

    deduped.push(label);
  }

  return deduped;
}

function dedupePriceLabels(labels: PriceLabelPoint[]): PriceLabelPoint[] {
  const deduped: PriceLabelPoint[] = [];

  for (const label of [...labels].sort((left, right) => left.y - right.y)) {
    const last = deduped.at(-1);
    if (last && Math.abs(last.y - label.y) < 8) {
      continue;
    }

    deduped.push(label);
  }

  return deduped;
}

function collectNearbyTextElements(root: ParentNode, chartRect: DOMRect): Element[] {
  return Array.from(root.querySelectorAll(TEXT_SELECTORS))
    .filter((element) => isVisibleElement(element) && isRectNearChart(element.getBoundingClientRect(), chartRect))
    .slice(0, MAX_TEXT_ELEMENTS);
}

function collectTimelineLabels(root: ParentNode, chartRect: DOMRect): TimestampLabelPoint[] {
  const labels: TimestampLabelPoint[] = [];

  for (const element of collectNearbyTextElements(root, chartRect)) {
    const rect = element.getBoundingClientRect();
    if (rect.top > chartRect.bottom + 120 || rect.bottom < chartRect.top - 40) {
      continue;
    }

    const parsedTimestamp = collectStringsFromElement(element)
      .map((text) => parseDateFromText(text))
      .find(Boolean);
    if (!parsedTimestamp) {
      continue;
    }

    labels.push({
      timestampMs: new Date(parsedTimestamp).getTime(),
      x: rect.left + rect.width / 2,
    });
  }

  return dedupeTimestampLabels(labels.filter((label) => !Number.isNaN(label.timestampMs)));
}

function collectPriceLabels(root: ParentNode, chartRect: DOMRect): PriceLabelPoint[] {
  const labels: PriceLabelPoint[] = [];

  for (const element of collectNearbyTextElements(root, chartRect)) {
    const rect = element.getBoundingClientRect();
    if (rect.bottom < chartRect.top - 20 || rect.top > chartRect.bottom + 20) {
      continue;
    }

    const parsedPrice = collectStringsFromElement(element)
      .map((text) => parsePriceFromText(text))
      .find((price): price is number => price !== undefined);
    if (parsedPrice === undefined) {
      continue;
    }

    labels.push({
      price: parsedPrice,
      y: rect.top + rect.height / 2,
    });
  }

  return dedupePriceLabels(labels);
}

function buildTimelineBounds(labels: TimestampLabelPoint[]): TimelineBounds | undefined {
  if (labels.length < 2) {
    return undefined;
  }

  const sorted = [...labels].sort((left, right) => left.x - right.x);
  const first = sorted[0];
  const last = sorted[sorted.length - 1];
  if (last.timestampMs <= first.timestampMs) {
    return undefined;
  }

  const labelSteps = sorted
    .slice(1)
    .map((label, index) => label.timestampMs - sorted[index].timestampMs)
    .filter((step) => step > 0);

  return {
    startMs: first.timestampMs,
    endMs: last.timestampMs,
    labelStepMs: median(labelSteps),
  };
}

function estimateTimestampFromLabels(
  clientX: number,
  chartRect: DOMRect,
  labels: TimestampLabelPoint[],
  bounds: TimelineBounds | undefined,
): string | undefined {
  const sorted = [...labels].sort((left, right) => left.x - right.x);

  if (sorted.length >= 2) {
    for (let index = 1; index < sorted.length; index += 1) {
      const left = sorted[index - 1];
      const right = sorted[index];
      if (clientX > right.x) {
        continue;
      }

      const ratio = estimateRatioFromClientX(clientX, left.x, right.x - left.x);
      return new Date(left.timestampMs + (right.timestampMs - left.timestampMs) * ratio).toISOString();
    }
  }

  if (!bounds) {
    return undefined;
  }

  return interpolateTimestamp(
    new Date(bounds.startMs).toISOString(),
    new Date(bounds.endMs).toISOString(),
    estimateRatioFromClientX(clientX, chartRect.left, chartRect.width),
  );
}

function estimatePriceFromAxis(clientY: number, labels: PriceLabelPoint[]): number | undefined {
  if (labels.length < 2) {
    return undefined;
  }

  const sorted = [...labels].sort((left, right) => left.y - right.y);
  const top = sorted[0];
  const bottom = sorted[sorted.length - 1];
  if (bottom.y <= top.y || top.price === bottom.price) {
    return undefined;
  }

  const ratio = Math.max(0, Math.min(1, (clientY - top.y) / (bottom.y - top.y)));
  const interpolated = top.price + (bottom.price - top.price) * ratio;
  return Math.max(0, Math.min(0.99, Number(interpolated.toFixed(2))));
}

function deriveTimeWindow(
  clickedTimestamp: string,
  bounds: TimelineBounds | undefined,
): { windowStart: string; windowEnd: string } {
  const center = new Date(clickedTimestamp).getTime();
  const fallbackCenter = Number.isNaN(center) ? Date.now() : center;
  const domainSpan = bounds ? bounds.endMs - bounds.startMs : undefined;
  const suggestedHalfWindow = bounds?.labelStepMs ? bounds.labelStepMs / 2 : domainSpan ? domainSpan / 24 : undefined;
  const halfWindowMs = Math.max(
    MIN_HALF_WINDOW_MS,
    Math.min(MAX_HALF_WINDOW_MS, suggestedHalfWindow ?? DEFAULT_HALF_WINDOW_MS),
  );

  return {
    windowStart: new Date(fallbackCenter - halfWindowMs).toISOString(),
    windowEnd: new Date(fallbackCenter + halfWindowMs).toISOString(),
  };
}

function buildFallbackContext(referenceDate: Date = new Date()): MarketClickContext {
  const metadata = extractMarketMetadata();

  return {
    marketId: metadata.marketId,
    marketTitle: metadata.marketTitle,
    marketQuestion: metadata.marketQuestion,
    marketSubtitle: metadata.marketSubtitle,
    marketRulesPrimary: metadata.marketRulesPrimary,
    clickedTimestamp: referenceDate.toISOString(),
    windowStart: new Date(referenceDate.getTime() - DEFAULT_HALF_WINDOW_MS).toISOString(),
    windowEnd: new Date(referenceDate.getTime() + DEFAULT_HALF_WINDOW_MS).toISOString(),
  };
}

async function buildResolvedFallbackContext(referenceDate: Date = new Date()): Promise<MarketClickContext> {
  const metadata = await resolveMarketMetadata();

  return {
    marketId: metadata.marketId,
    marketTitle: metadata.marketTitle,
    marketQuestion: metadata.marketQuestion,
    marketSubtitle: metadata.marketSubtitle,
    marketRulesPrimary: metadata.marketRulesPrimary,
    clickedTimestamp: referenceDate.toISOString(),
    windowStart: new Date(referenceDate.getTime() - DEFAULT_HALF_WINDOW_MS).toISOString(),
    windowEnd: new Date(referenceDate.getTime() + DEFAULT_HALF_WINDOW_MS).toISOString(),
  };
}

function resolveChartTargets(click: ClickSnapshot): { chartElement: Element | null; chartContainer: Element | null } {
  const pointElement = collectPointElements(click)
    .map((element) => findClosestChartElement(element))
    .find((element): element is Element => Boolean(element));
  const targetElement = click.target ? findClosestChartElement(click.target) : null;
  const chartElement = pointElement ?? targetElement ?? findLikelyChartElement();

  const pointContainer = collectPointElements(click)
    .map((element) => findClosestChartContainer(element))
    .find((element): element is Element => Boolean(element));
  const targetContainer = click.target ? findClosestChartContainer(click.target) : null;
  const chartContainer = pointContainer ?? targetContainer ?? findLikelyChartContainer(chartElement ?? document);

  return {
    chartElement,
    chartContainer,
  };
}

function buildContextFromClick(click: ClickSnapshot, fallbackContext: MarketClickContext): MarketClickContext {
  const { chartElement, chartContainer } = resolveChartTargets(click);

  if (!chartElement) {
    return fallbackContext;
  }

  // Kalshi's live chart data is not reliably exposed to the extension world, so if no
  // tooltip payload is available we approximate the clicked point from visible labels.
  const chartRect = chartElement.getBoundingClientRect();
  const searchRoot = chartContainer ?? chartElement;
  const strings = collectContextStrings(click, chartRect);
  const tooltipTimestamp = strings.map((text) => parseDateFromText(text)).find(Boolean);
  const tooltipPrice = strings.map((text) => parsePriceFromText(text)).find((price): price is number => price !== undefined);
  const timelineLabels = collectTimelineLabels(searchRoot, chartRect);
  const timelineBounds = buildTimelineBounds(timelineLabels);
  const clickedTimestamp =
    tooltipTimestamp ??
    estimateTimestampFromLabels(click.clientX, chartRect, timelineLabels, timelineBounds) ??
    fallbackContext.clickedTimestamp;
  const priceLabels = collectPriceLabels(searchRoot, chartRect);
  const clickedPrice = tooltipPrice ?? estimatePriceFromAxis(click.clientY, priceLabels);
  const { windowStart, windowEnd } = deriveTimeWindow(clickedTimestamp, timelineBounds);

  return {
    ...fallbackContext,
    clickedTimestamp,
    clickedPrice,
    windowStart,
    windowEnd,
  };
}

function getPrimaryEventElement(event: MouseEvent): Element | null {
  const pathTarget = event.composedPath().find((candidate): candidate is Element => candidate instanceof Element);
  return pathTarget ?? (event.target instanceof Element ? event.target : null);
}

export function initializeChartCapture(callbacks: ChartCaptureCallbacks): void {
  document.addEventListener(
    "click",
    (event) => {
      const target = getPrimaryEventElement(event);
      if (!isClickInsideChart(target)) {
        return;
      }

      const clickSnapshot: ClickSnapshot = {
        clientX: event.clientX,
        clientY: event.clientY,
        target,
      };

      window.setTimeout(() => {
        void buildResolvedFallbackContext()
          .then((fallbackContext) => callbacks.onContext(buildContextFromClick(clickSnapshot, fallbackContext)))
          .catch(() => callbacks.onContext(buildFallbackContext()));
      }, 0);
    },
    true,
  );
}
