export interface MarketMetadata {
  marketId: string;
  marketTitle: string;
  marketQuestion: string;
  marketSubtitle?: string;
  marketRulesPrimary?: string;
}

const KALSHI_API_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2";

function normalizeText(value: string | null | undefined): string | undefined {
  const normalized = value?.replace(/\s+/g, " ").trim();
  return normalized || undefined;
}

function deepFindString(input: unknown, preferredKeys: string[]): string | undefined {
  const queue: unknown[] = [input];

  while (queue.length > 0) {
    const current = queue.shift();

    if (!current || typeof current !== "object") {
      continue;
    }

    if (Array.isArray(current)) {
      queue.push(...current);
      continue;
    }

    const record = current as Record<string, unknown>;
    for (const key of preferredKeys) {
      if (typeof record[key] === "string" && record[key].trim()) {
        return record[key] as string;
      }
    }

    queue.push(...Object.values(record));
  }

  return undefined;
}

export function deepFindPreferredString(input: unknown, preferredKeys: string[]): string | undefined {
  const queue: unknown[] = [input];
  const firstMatchByKey = new Map<string, string>();
  const keySet = new Set(preferredKeys);

  for (let index = 0; index < queue.length; index += 1) {
    const current = queue[index];

    if (!current || typeof current !== "object") {
      continue;
    }

    if (Array.isArray(current)) {
      queue.push(...current);
      continue;
    }

    const record = current as Record<string, unknown>;
    for (const [key, value] of Object.entries(record)) {
      if (!keySet.has(key) || firstMatchByKey.has(key)) {
        continue;
      }
      if (typeof value === "string" && value.trim()) {
        firstMatchByKey.set(key, value);
      }
    }

    if (firstMatchByKey.size === keySet.size) {
      break;
    }

    queue.push(...Object.values(record));
  }

  for (const key of preferredKeys) {
    const match = firstMatchByKey.get(key);
    if (match) {
      return match;
    }
  }

  return undefined;
}

interface MarketObject {
  ticker?: string;
  market_ticker?: string;
  event_ticker?: string;
  title?: string;
  question?: string;
  rules_primary?: string;
  subtitle?: string;
  description?: string;
  name?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function metadataFromMarketObject(marketObj: MarketObject | undefined): Partial<MarketMetadata> {
  if (!marketObj) {
    return {};
  }

  const marketId = normalizeText(marketObj.ticker || marketObj.market_ticker || marketObj.event_ticker);
  const marketTitle = normalizeText(marketObj.title || marketObj.name || marketObj.subtitle);
  const marketSubtitle = normalizeText(marketObj.subtitle);
  const marketRulesPrimary = normalizeText(marketObj.rules_primary);
  const marketQuestion = normalizeText(
    marketObj.question || marketSubtitle || marketObj.description || marketRulesPrimary || marketTitle,
  );

  return {
    marketId,
    marketTitle,
    marketQuestion,
    marketSubtitle,
    marketRulesPrimary,
  };
}

function findMarketObject(input: unknown, urlTicker: string | undefined): MarketObject | undefined {
  if (!urlTicker) return undefined;

  const queue: unknown[] = [input];
  const tickerLower = urlTicker.toLowerCase();

  while (queue.length > 0) {
    const current = queue.shift();

    if (!current || typeof current !== "object") {
      continue;
    }

    if (Array.isArray(current)) {
      queue.push(...current);
      continue;
    }

    const record = current as Record<string, unknown>;
    const recordTicker = record.ticker || record.market_ticker || record.event_ticker;

    if (typeof recordTicker === "string" && recordTicker.toLowerCase().includes(tickerLower)) {
      return record as MarketObject;
    }

    queue.push(...Object.values(record));
  }

  return undefined;
}

function extractMetaContent(selectors: string[]): string | undefined {
  for (const selector of selectors) {
    const content = document.querySelector<HTMLMetaElement>(selector)?.content;
    const normalized = normalizeText(content);
    if (normalized) {
      return normalized;
    }
  }

  return undefined;
}

function parseNextData(): Record<string, unknown> | undefined {
  const script = document.getElementById("__NEXT_DATA__");
  if (!script?.textContent) {
    return undefined;
  }

  try {
    return JSON.parse(script.textContent) as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

export function inferTickerFromScriptTexts(scriptTexts: string[]): string | undefined {
  const keyPatterns = [
    /"market_ticker"\s*:\s*"([^"]+)"/,
    /"ticker"\s*:\s*"([^"]+)"/,
    /"(?:event_ticker)"\s*:\s*"([^"]+)"/,
  ];

  for (const pattern of keyPatterns) {
    for (const scriptText of scriptTexts) {
      const match = scriptText.match(pattern);
      if (match?.[1]) {
        return match[1];
      }
    }
  }

  return undefined;
}

function inferTickerFromScripts(): string | undefined {
  const scripts = Array.from(document.scripts)
    .map((script) => script.textContent ?? "")
    .filter(Boolean);

  return inferTickerFromScriptTexts(scripts);
}

function isLikelyKalshiMarketId(segment: string): boolean {
  return /^k[a-z0-9-]{5,}$/i.test(segment);
}

export function inferTickerFromUrl(url: URL): string | undefined {
  const segments = url.pathname.split("/").map((segment) => segment.trim()).filter(Boolean);
  return [...segments].reverse().find((segment) => isLikelyKalshiMarketId(segment));
}

function inferTickerFromDom(): string | undefined {
  if (typeof document === "undefined") {
    return undefined;
  }

  const selectors = ["[data-market-id]", "[data-market-ticker]", "[data-ticker]"];

  for (const selector of selectors) {
    const element = document.querySelector(selector);
    if (!(element instanceof HTMLElement)) {
      continue;
    }

    const attributes = ["data-market-id", "data-market-ticker", "data-ticker"];
    for (const attribute of attributes) {
      const value = normalizeText(element.getAttribute(attribute));
      if (value) {
        return value;
      }
    }
  }

  return undefined;
}

function inferQuestionFromPage(title: string): string {
  const heading = normalizeText(document.querySelector("h1")?.textContent);
  if (heading?.includes("?")) {
    return heading;
  }

  const candidateParagraph = Array.from(document.querySelectorAll("p, h2, h3"))
    .map((element) => normalizeText(element.textContent) ?? "")
    .find((text) => text.includes("?"));

  return (
    candidateParagraph ||
    extractMetaContent(["meta[name='description']", "meta[property='og:description']"]) ||
    title
  );
}

export function resolvePreferredMarketId(nextData: unknown, scriptTexts: string[], url: URL): string | undefined {
  const marketTicker =
    deepFindPreferredString(nextData, ["market_ticker"]) ||
    inferTickerFromScriptTexts(
      scriptTexts.filter((scriptText) => /"market_ticker"\s*:/.test(scriptText)),
    );

  return (
    marketTicker ||
    inferTickerFromDom() ||
    inferTickerFromUrl(url) ||
    deepFindPreferredString(nextData, ["ticker", "event_ticker"]) ||
    inferTickerFromScriptTexts(scriptTexts.filter((scriptText) => /"(?:ticker|event_ticker)"\s*:/.test(scriptText)))
  );
}

export function extractMarketMetadata(): MarketMetadata {
  const nextData = parseNextData();
  const url = new URL(window.location.href);
  const urlTicker = inferTickerFromUrl(url);
  const fallbackId = urlTicker || url.pathname.split("/").filter(Boolean).join(":") || url.hostname;
  const pageHeading = normalizeText(document.querySelector("h1")?.textContent);
  const metaTitle = extractMetaContent(["meta[property='og:title']", "meta[name='twitter:title']"]);
  const documentTitle = normalizeText(document.title.replace(/\s*\|\s*Kalshi\s*$/i, ""));
  const domTitle = pageHeading || metaTitle || documentTitle || fallbackId;

  // Try to find the specific market object matching the URL ticker
  const marketObj = findMarketObject(nextData, urlTicker);

  // Extract from the matched market object first, then fall back to deep search
  const marketId =
    marketObj?.ticker ||
    marketObj?.market_ticker ||
    marketObj?.event_ticker ||
    resolvePreferredMarketId(
      nextData,
      Array.from(document.scripts).map((script) => script.textContent ?? "").filter(Boolean),
      url,
    ) ||
    fallbackId;

  const marketMetadata = metadataFromMarketObject(marketObj);
  const marketTitle = marketMetadata.marketTitle || pageHeading || metaTitle || documentTitle || fallbackId;
  const marketSubtitle = marketMetadata.marketSubtitle;
  const marketRulesPrimary = marketMetadata.marketRulesPrimary;

  // Prefer the matched market object, then fall back to the title rather than broad page text.
  const marketQuestion =
    marketMetadata.marketQuestion ||
    marketTitle;

  return {
    marketId,
    marketTitle,
    marketQuestion,
    marketSubtitle,
    marketRulesPrimary,
  };
}

function parseMarketResponse(payload: unknown): MarketObject | undefined {
  if (!isRecord(payload)) {
    return undefined;
  }

  if (isRecord(payload.market)) {
    return payload.market as MarketObject;
  }

  return payload as MarketObject;
}

function mergeMetadata(
  fallback: MarketMetadata,
  authoritative: Partial<MarketMetadata>,
): MarketMetadata {
  const mergedTitle = authoritative.marketTitle || fallback.marketTitle;
  const mergedSubtitle = authoritative.marketSubtitle || fallback.marketSubtitle;
  const mergedRulesPrimary = authoritative.marketRulesPrimary || fallback.marketRulesPrimary;
  const mergedQuestion =
    authoritative.marketQuestion ||
    fallback.marketQuestion ||
    mergedSubtitle ||
    mergedRulesPrimary ||
    mergedTitle;

  return {
    marketId: authoritative.marketId || fallback.marketId,
    marketTitle: mergedTitle,
    marketQuestion: mergedQuestion,
    marketSubtitle: mergedSubtitle,
    marketRulesPrimary: mergedRulesPrimary,
  };
}

export async function resolveMarketMetadata(
  fetchImpl: typeof fetch = fetch,
): Promise<MarketMetadata> {
  const fallback = extractMarketMetadata();
  if (!fallback.marketId) {
    return fallback;
  }

  try {
    const response = await fetchImpl(
      `${KALSHI_API_BASE_URL}/markets/${encodeURIComponent(fallback.marketId)}`,
      {
        method: "GET",
        credentials: "omit",
      },
    );
    if (!response.ok) {
      return fallback;
    }

    const payload = (await response.json()) as unknown;
    const market = parseMarketResponse(payload);
    return mergeMetadata(fallback, metadataFromMarketObject(market));
  } catch {
    return fallback;
  }
}
