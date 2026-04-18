const CHART_ELEMENT_SELECTORS = [
  "[data-testid*='chart' i] svg",
  "[data-testid*='history' i] svg",
  "[class*='chart' i] svg",
  "[class*='history' i] svg",
  "[data-testid*='chart' i] canvas",
  "[data-testid*='history' i] canvas",
  "[class*='chart' i] canvas",
  "[class*='history' i] canvas",
  "svg",
  "canvas",
];

const CHART_CONTAINER_SELECTORS = [
  "[data-testid*='chart' i]",
  "[data-testid*='history' i]",
  "[aria-label*='chart' i]",
  "[class*='chart' i]",
  "[class*='history' i]",
];

function collectMatches(root: ParentNode, selectors: string[]): Element[] {
  const matches: Element[] = [];

  if (root instanceof Element) {
    for (const selector of selectors) {
      if (root.matches(selector)) {
        matches.push(root);
      }
    }
  }

  for (const selector of selectors) {
    matches.push(...Array.from(root.querySelectorAll(selector)));
  }

  return Array.from(new Set(matches));
}

function isVisible(element: Element): boolean {
  const rect = element.getBoundingClientRect();
  if (rect.width <= 200 || rect.height <= 120) {
    return false;
  }

  const style = window.getComputedStyle(element);
  return style.display !== "none" && style.visibility !== "hidden" && style.opacity !== "0";
}

function getLargestVisibleElement(candidates: Element[]): Element | null {
  const visibleCandidates = candidates.filter(isVisible);
  if (!visibleCandidates.length) {
    return null;
  }

  return (
    visibleCandidates
      .sort((left, right) => {
        const leftRect = left.getBoundingClientRect();
        const rightRect = right.getBoundingClientRect();
        return rightRect.width * rightRect.height - leftRect.width * leftRect.height;
      })
      .at(0) ?? null
  );
}

export function isKalshiPage(url: URL = new URL(window.location.href)): boolean {
  return url.hostname.includes("kalshi.com");
}

export function isLikelyMarketPage(url: URL = new URL(window.location.href)): boolean {
  return isKalshiPage(url) && (url.pathname.includes("/markets/") || url.pathname.split("/").filter(Boolean).length > 0);
}

export function findLikelyChartElement(root: ParentNode = document): Element | null {
  return getLargestVisibleElement(collectMatches(root, CHART_ELEMENT_SELECTORS));
}

export function findLikelyChartContainer(root: ParentNode = document): Element | null {
  const chart = findLikelyChartElement(root);
  if (!chart) {
    return null;
  }

  const explicitContainer = CHART_CONTAINER_SELECTORS
    .map((selector) => chart.closest(selector))
    .find((candidate): candidate is Element => Boolean(candidate && isVisible(candidate)));

  return explicitContainer ?? chart.parentElement ?? chart;
}

export function findClosestChartElement(target: EventTarget | null): Element | null {
  if (!(target instanceof Element)) {
    return findLikelyChartElement();
  }

  for (const selector of CHART_ELEMENT_SELECTORS) {
    const match = target.closest(selector);
    if (match && isVisible(match)) {
      return match;
    }
  }

  for (const selector of CHART_CONTAINER_SELECTORS) {
    const container = target.closest(selector);
    if (!container || !isVisible(container)) {
      continue;
    }

    const localChart = findLikelyChartElement(container);
    if (localChart) {
      return localChart;
    }
  }

  const localChart = findLikelyChartElement(target);
  if (localChart && localChart.contains(target)) {
    return localChart;
  }

  return null;
}

export function findClosestChartContainer(target: EventTarget | null): Element | null {
  if (!(target instanceof Element)) {
    return findLikelyChartContainer();
  }

  for (const selector of CHART_CONTAINER_SELECTORS) {
    const match = target.closest(selector);
    if (match && isVisible(match)) {
      return match;
    }
  }

  const chart = findClosestChartElement(target);
  if (!chart) {
    return findLikelyChartContainer();
  }

  return findLikelyChartContainer(chart) ?? chart.parentElement ?? chart;
}

export function isClickInsideChart(target: EventTarget | null): target is Element {
  if (!(target instanceof Element)) {
    return false;
  }

  const chart = findClosestChartElement(target) ?? findLikelyChartElement();
  return Boolean(chart && (target === chart || chart.contains(target)));
}
